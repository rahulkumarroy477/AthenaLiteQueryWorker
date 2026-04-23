package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SqlValidatorTest {

    @Test
    void allowsSimpleSelect() {
        assertNull(SqsQueryHandler.validateSql("SELECT * FROM users"));
    }

    @Test
    void allowsSelectWithWhere() {
        assertNull(SqsQueryHandler.validateSql("SELECT name FROM users WHERE id = 1"));
    }

    @Test
    void allowsWithCte() {
        assertNull(SqsQueryHandler.validateSql("WITH cte AS (SELECT * FROM users) SELECT * FROM cte"));
    }

    @Test
    void allowsSelectWithLimit() {
        assertNull(SqsQueryHandler.validateSql("SELECT * FROM users LIMIT 10"));
    }

    @Test
    void allowsLeadingComment() {
        assertNull(SqsQueryHandler.validateSql("-- comment\nSELECT * FROM users"));
    }

    @Test
    void allowsBlockComment() {
        assertNull(SqsQueryHandler.validateSql("/* comment */ SELECT * FROM users"));
    }

    @Test
    void blocksDrop() {
        assertNotNull(SqsQueryHandler.validateSql("DROP TABLE users"));
    }

    @Test
    void blocksInsert() {
        assertNotNull(SqsQueryHandler.validateSql("INSERT INTO users VALUES (1, 'a')"));
    }

    @Test
    void blocksDelete() {
        assertNotNull(SqsQueryHandler.validateSql("DELETE FROM users"));
    }

    @Test
    void blocksUpdate() {
        assertNotNull(SqsQueryHandler.validateSql("UPDATE users SET name='x'"));
    }

    @Test
    void blocksCopy() {
        assertNotNull(SqsQueryHandler.validateSql("COPY users TO '/tmp/out.csv'"));
    }

    @Test
    void blocksInstall() {
        assertNotNull(SqsQueryHandler.validateSql("INSTALL httpfs"));
    }

    @Test
    void blocksAttach() {
        assertNotNull(SqsQueryHandler.validateSql("ATTACH '/tmp/db' AS stolen"));
    }

    @Test
    void blocksSet() {
        assertNotNull(SqsQueryHandler.validateSql("SET enable_external_access=true"));
    }

    @Test
    void blocksSemicolonChainedDrop() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT 1; DROP TABLE users"));
    }

    @Test
    void blocksSemicolonChainedCopy() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT 1; COPY users TO '/tmp/x'"));
    }

    @Test
    void blocksReadCsv() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT * FROM read_csv('/etc/passwd')"));
    }

    @Test
    void blocksReadParquet() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT * FROM read_parquet('s3://bucket/key')"));
    }

    @Test
    void blocksReadJson() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT * FROM read_json_auto('/tmp/data.json')"));
    }

    @Test
    void blocksGlob() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT * FROM glob('/tmp/*')"));
    }

    @Test
    void blocksReadCsvInSubquery() {
        assertNotNull(SqsQueryHandler.validateSql("SELECT * FROM (SELECT * FROM read_csv('/etc/passwd'))"));
    }

    @Test
    void blocksEmptyQuery() {
        assertNotNull(SqsQueryHandler.validateSql(""));
        assertNotNull(SqsQueryHandler.validateSql("   "));
        assertNotNull(SqsQueryHandler.validateSql(null));
    }

    @Test
    void blocksCommentOnly() {
        assertNotNull(SqsQueryHandler.validateSql("-- just a comment"));
    }
}
