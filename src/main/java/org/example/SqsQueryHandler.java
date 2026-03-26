package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.sql.*;

public class SqsQueryHandler implements RequestHandler<SQSEvent, Void> {

    private static final Logger log = LoggerFactory.getLogger(SqsQueryHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String REGION = System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "ap-south-1";
    private static final String BUCKET = System.getenv("S3_BUCKET") != null ? System.getenv("S3_BUCKET") : "athenalite-data-ap";
    private static final String QUERY_TABLE = System.getenv("DYNAMODB_QUERY_TABLE") != null ? System.getenv("DYNAMODB_QUERY_TABLE") : "AthenaLiteQueryMetadata";

    private final S3Client s3 = S3Client.builder().region(Region.of(REGION)).build();
    private final DynamoDbTable<QueryMetadata> queryTable;

    public SqsQueryHandler() {
        DynamoDbClient ddb = DynamoDbClient.builder().region(Region.of(REGION)).build();
        DynamoDbEnhancedClient enhanced = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();
        this.queryTable = enhanced.table(QUERY_TABLE, TableSchema.fromBean(QueryMetadata.class));
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

        String queryId = job.getResultKey().replaceAll(".*/", "").replace(".csv", "");
        long start = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute("SET home_directory='/tmp'");
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
            stmt.execute("SET s3_region='" + REGION + "'");

            String s3Input = "s3://" + BUCKET + "/" + job.getS3ParquetKey();
            log.info("Creating view from {}", s3Input);
            stmt.execute("CREATE VIEW \"" + job.getTableName() + "\" AS SELECT * FROM read_parquet('" + s3Input + "')");

            log.info("Executing: {}", job.getSql());
            ResultSet rs = stmt.executeQuery(job.getSql());

            // Build CSV from results
            StringBuilder csv = new StringBuilder();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            for (int i = 1; i <= colCount; i++) {
                if (i > 1) csv.append(",");
                csv.append(meta.getColumnName(i));
            }
            csv.append("\n");

            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) csv.append(",");
                    String val = rs.getString(i);
                    if (val != null && val.contains(",")) csv.append("\"").append(val).append("\"");
                    else csv.append(val != null ? val : "");
                }
                csv.append("\n");
            }

            // Upload CSV to S3
            s3.putObject(
                    PutObjectRequest.builder().bucket(BUCKET).key(job.getResultKey()).contentType("text/csv").build(),
                    RequestBody.fromString(csv.toString()));
            log.info("Results written to {}", job.getResultKey());

            long elapsed = System.currentTimeMillis() - start;
            updateQuery(job.getUserId(), queryId, "COMPLETED", elapsed + "ms", null);

        } catch (Exception e) {
            log.error("Query failed: {}", e.getMessage(), e);
            long elapsed = System.currentTimeMillis() - start;
            updateQuery(job.getUserId(), queryId, "FAILED", elapsed + "ms", e.getMessage());
        }
    }

    private void updateQuery(String userId, String queryId, String status, String executionTime, String error) {
        QueryMetadata qm = new QueryMetadata();
        qm.setUserId(userId);
        qm.setQueryId(queryId);
        qm.setStatus(status);
        qm.setExecutionTime(executionTime);
        qm.setError(error != null ? error : "");
        queryTable.putItem(qm);
    }
}
