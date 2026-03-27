package org.example;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

@DynamoDbBean
public class TableMetadata {

    private String userId;
    private String tableName;
    private String s3ParquetKey;
    private String status;

    @DynamoDbPartitionKey
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    @DynamoDbSortKey
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getS3ParquetKey() { return s3ParquetKey; }
    public void setS3ParquetKey(String s3ParquetKey) { this.s3ParquetKey = s3ParquetKey; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
