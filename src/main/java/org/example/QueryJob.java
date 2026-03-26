package org.example;

public class QueryJob {
    private String userId;
    private String tableName;
    private String sql;
    private String s3ParquetKey;
    private String resultKey;

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
    public String getS3ParquetKey() { return s3ParquetKey; }
    public void setS3ParquetKey(String s3ParquetKey) { this.s3ParquetKey = s3ParquetKey; }
    public String getResultKey() { return resultKey; }
    public void setResultKey(String resultKey) { this.resultKey = resultKey; }
}
