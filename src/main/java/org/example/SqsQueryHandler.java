package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class SqsQueryHandler implements RequestHandler<SQSEvent, Void> {

    private static final Logger log = LoggerFactory.getLogger(SqsQueryHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String REGION = System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "ap-south-1";
    private static final String BUCKET = System.getenv("S3_BUCKET") != null ? System.getenv("S3_BUCKET") : "athenalite-data-ap";
    private static final String QUERY_TABLE = System.getenv("DYNAMODB_QUERY_TABLE") != null ? System.getenv("DYNAMODB_QUERY_TABLE") : "AthenaLiteQueryMetadata";
    private static final String METADATA_TABLE = System.getenv("DYNAMODB_TABLE") != null ? System.getenv("DYNAMODB_TABLE") : "AthenaLiteTables";

    private static final int MAX_RESULT_ROWS = 100_000;
    private static final int MAX_ERROR_LENGTH = 500;

    // Blocked SQL keywords (case-insensitive check on trimmed, uppercased SQL)
    private static final Set<String> BLOCKED_PREFIXES = Set.of(
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "COPY",
            "EXPORT", "IMPORT", "INSTALL", "LOAD", "ATTACH", "DETACH",
            "CALL", "EXECUTE", "PRAGMA", "SET", "BEGIN", "COMMIT", "ROLLBACK",
            "GRANT", "REVOKE", "VACUUM", "CHECKPOINT"
    );

    // Block dangerous functions anywhere in the query
    private static final Pattern DANGEROUS_FUNCTIONS = Pattern.compile(
            "\\b(read_csv|read_csv_auto|read_json|read_json_auto|read_parquet|read_blob|"
                    + "read_text|glob|system|getenv|current_setting|pg_read_file|"
                    + "write_csv|write_parquet|httpfs_|http_get|http_post)\\s*\\(",
            Pattern.CASE_INSENSITIVE
    );

    private final S3Client s3 = S3Client.builder().region(Region.of(REGION)).build();
    private final DynamoDbTable<QueryMetadata> queryTable;
    private final DynamoDbTable<TableMetadata> metadataTable;

    public SqsQueryHandler() {
        DynamoDbClient ddb = DynamoDbClient.builder().region(Region.of(REGION)).build();
        DynamoDbEnhancedClient enhanced = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();
        this.queryTable = enhanced.table(QUERY_TABLE, TableSchema.fromBean(QueryMetadata.class));
        this.metadataTable = enhanced.table(METADATA_TABLE, TableSchema.fromBean(TableMetadata.class));
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        for (SQSEvent.SQSMessage msg : event.getRecords()) {
            processMessage(msg.getBody());
        }
        return null;
    }

    private void processMessage(String body) {
        QueryJob job;
        try {
            job = mapper.readValue(body, QueryJob.class);
        } catch (Exception e) {
            log.error("Failed to parse SQS message: {}", e.getMessage());
            return;
        }

        // Validate required fields
        if (job.getResultKey() == null || job.getUserId() == null || job.getSql() == null) {
            log.error("Invalid QueryJob: missing required fields");
            return;
        }

        String queryId = job.getResultKey().replaceAll(".*/", "").replace(".csv", "");
        long start = System.currentTimeMillis();

        // Validate SQL before executing
        String sqlValidationError = validateSql(job.getSql());
        if (sqlValidationError != null) {
            updateQuery(job.getUserId(), queryId, "FAILED", "0ms", sqlValidationError);
            return;
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            // Phase 1: Setup with external access (needed for httpfs + view creation)
            stmt.execute("SET home_directory='/tmp'");
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
            stmt.execute("SET s3_region='" + escapeSqlString(REGION) + "'");

            // Resource limits
            stmt.execute("SET memory_limit='512MB'");
            stmt.execute("SET threads=2");

            // Load ALL ready tables for this user and create views
            List<TableMetadata> userTables = metadataTable.query(
                    QueryConditional.keyEqualTo(k -> k.partitionValue(job.getUserId()))
            ).items().stream().filter(t -> "READY".equals(t.getStatus())).toList();

            for (TableMetadata t : userTables) {
                String s3Path = "s3://" + BUCKET + "/" + escapeSqlString(t.getS3ParquetKey());
                String safeName = escapeSqlIdentifier(t.getTableName());
                log.info("Creating view \"{}\" from {}", safeName, s3Path);
                stmt.execute("CREATE VIEW \"" + safeName + "\" AS SELECT * FROM read_parquet('" + s3Path + "')");
            }

            // Phase 2: Execute user SQL (views resolve read_parquet at query time, so external access must remain on)
            log.info("Executing query for user={}, queryId={}", job.getUserId(), queryId);
            ResultSet rs = stmt.executeQuery(job.getSql());

            // Build CSV from results with row limit
            StringBuilder csv = new StringBuilder();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            for (int i = 1; i <= colCount; i++) {
                if (i > 1) csv.append(",");
                csv.append(escapeCsvField(meta.getColumnName(i)));
            }
            csv.append("\n");

            int rowCount = 0;
            while (rs.next() && rowCount < MAX_RESULT_ROWS) {
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) csv.append(",");
                    String val = rs.getString(i);
                    csv.append(val != null ? escapeCsvField(val) : "");
                }
                csv.append("\n");
                rowCount++;
            }

            s3.putObject(
                    PutObjectRequest.builder().bucket(BUCKET).key(job.getResultKey()).contentType("text/csv").build(),
                    RequestBody.fromString(csv.toString()));
            log.info("Results written to {}, rows={}", job.getResultKey(), rowCount);

            long elapsed = System.currentTimeMillis() - start;
            updateQuery(job.getUserId(), queryId, "COMPLETED", elapsed + "ms", null);

        } catch (Exception e) {
            log.error("Query failed for user={}, queryId={}: {}", job.getUserId(), queryId, e.getMessage(), e);
            long elapsed = System.currentTimeMillis() - start;
            updateQuery(job.getUserId(), queryId, "FAILED", elapsed + "ms", sanitizeError(e.getMessage()));
        } finally {
            cleanupTmp();
        }
    }

    /**
     * Validates that the SQL is a safe SELECT query.
     * Returns null if valid, or an error message if blocked.
     */
    static String validateSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "Empty query";
        }

        String trimmed = sql.trim();

        // Strip leading comments (-- and /* */)
        while (trimmed.startsWith("--") || trimmed.startsWith("/*")) {
            if (trimmed.startsWith("--")) {
                int nl = trimmed.indexOf('\n');
                trimmed = nl >= 0 ? trimmed.substring(nl + 1).trim() : "";
            } else {
                int end = trimmed.indexOf("*/");
                trimmed = end >= 0 ? trimmed.substring(end + 2).trim() : "";
            }
        }

        if (trimmed.isEmpty()) {
            return "Empty query";
        }

        // Must start with SELECT or WITH (for CTEs)
        String upper = trimmed.toUpperCase();
        if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
            return "Only SELECT queries are allowed";
        }

        // Check for blocked keywords as statement starters (handles ; chaining)
        // Split on semicolons and check each statement
        String[] statements = trimmed.split(";");
        for (int i = 1; i < statements.length; i++) {
            String part = statements[i].trim().toUpperCase();
            if (part.isEmpty()) continue;
            for (String blocked : BLOCKED_PREFIXES) {
                if (part.startsWith(blocked)) {
                    return "Only SELECT queries are allowed";
                }
            }
            // Additional statements after ; must also be SELECT/WITH or empty
            if (!part.startsWith("SELECT") && !part.startsWith("WITH") && !part.isEmpty()) {
                return "Only SELECT queries are allowed";
            }
        }

        // Check for dangerous functions anywhere in the query
        if (DANGEROUS_FUNCTIONS.matcher(sql).find()) {
            return "Query contains blocked functions";
        }

        return null;
    }

    /** Escapes a value for use inside a SQL single-quoted string */
    private static String escapeSqlString(String val) {
        return val.replace("'", "''");
    }

    /** Escapes a value for use inside a SQL double-quoted identifier */
    private static String escapeSqlIdentifier(String val) {
        return val.replace("\"", "\"\"");
    }

    /** Properly escapes a CSV field (handles commas, quotes, newlines, formula injection) */
    private static String escapeCsvField(String val) {
        if (val == null) return "";
        // Prevent CSV formula injection
        if (!val.isEmpty() && "=+-@\t\r".indexOf(val.charAt(0)) >= 0) {
            val = "'" + val;
        }
        if (val.contains(",") || val.contains("\"") || val.contains("\n") || val.contains("\r")) {
            return "\"" + val.replace("\"", "\"\"") + "\"";
        }
        return val;
    }

    /** Sanitize error messages to avoid leaking internal details */
    private static String sanitizeError(String error) {
        if (error == null) return "Unknown error";
        // Remove file paths, S3 paths, class names
        String sanitized = error
                .replaceAll("/tmp/[^\\s]+", "[internal]")
                .replaceAll("s3://[^\\s]+", "[s3-path]")
                .replaceAll("\\b[a-z]+\\.[a-z]+\\.[A-Z][a-zA-Z]*Exception", "Error");
        if (sanitized.length() > MAX_ERROR_LENGTH) {
            sanitized = sanitized.substring(0, MAX_ERROR_LENGTH) + "...";
        }
        return sanitized;
    }

    /** Clean up DuckDB temp files from /tmp */
    private static void cleanupTmp() {
        try {
            File tmp = new File("/tmp");
            File[] duckFiles = tmp.listFiles((dir, name) ->
                    name.startsWith(".duckdb") || name.startsWith("duckdb_"));
            if (duckFiles != null) {
                for (File f : duckFiles) {
                    f.delete();
                }
            }
        } catch (Exception ignored) {}
    }

    private void updateQuery(String userId, String queryId, String status, String executionTime, String error) {
        QueryMetadata qm = new QueryMetadata();
        qm.setUserId(userId);
        qm.setQueryId(queryId);
        qm.setStatus(status);
        qm.setExecutionTime(executionTime);
        qm.setError(error != null ? error : "");
        qm.setTtl(java.time.LocalDate.now(java.time.ZoneOffset.UTC).plusDays(1)
                .atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond());
        queryTable.putItem(qm);
    }
}
