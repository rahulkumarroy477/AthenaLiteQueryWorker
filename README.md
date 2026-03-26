# AthenaLite-QueryWorker

SQS-triggered Java Lambda that executes SQL queries using DuckDB against Parquet files in S3.

## Architecture Role

Consumes query jobs from `AthenaLiteQueryQueue`. For each job it:

1. Reads the `QueryJob` JSON from SQS
2. Spins up an in-memory DuckDB instance
3. Loads the `httpfs` extension to read directly from S3
4. Creates a view over the Parquet file
5. Executes the user's SQL query
6. Writes results as CSV to S3
7. Updates query status in DynamoDB (`COMPLETED` or `FAILED`)

## SQS Message Format (QueryJob)

```json
{
  "userId": "user@example.com",
  "tableName": "sales_data",
  "sql": "SELECT * FROM \"sales_data\" LIMIT 10",
  "s3ParquetKey": "parquet/user@example.com/sales_data.parquet",
  "resultKey": "results/user@example.com/qr_1234567890.csv"
}
```

## S3 Key Structure

```
parquet/{userId}/{tableName}.parquet    ← input (read by DuckDB)
results/{userId}/{queryId}.csv          ← output (query results)
```

## DynamoDB Record Updated

Table: `AthenaLiteQueryMetadata`

| Field | Example |
|-------|---------|
| userId | `user@example.com` |
| queryId | `qr_1234567890` |
| status | `COMPLETED` or `FAILED` |
| executionTime | `450ms` |
| error | error message if failed |

## Environment Variables

| Variable | Value |
|----------|-------|
| `S3_BUCKET` | `athenalite-data-ap` |
| `DYNAMODB_QUERY_TABLE` | `AthenaLiteQueryMetadata` |

`AWS_REGION` is set automatically by Lambda.

## Lambda Configuration

| Setting | Value |
|---------|-------|
| Runtime | Java 17 |
| Handler | `org.example.SqsQueryHandler::handleRequest` |
| Memory | 1024 MB |
| Timeout | 120 seconds |
| Trigger | SQS `AthenaLiteQueryQueue` (batch size 1) |

## Build & Deploy

```bash
mvn clean package
# Upload target/AthenaLiteQueryWorker-1.0-SNAPSHOT-lambda-package.zip to Lambda
```

## Tech Stack

- DuckDB (JDBC, with httpfs extension for S3 access)
- AWS Lambda (SQS trigger)
- AWS S3 SDK v2
- AWS DynamoDB Enhanced Client
- Jackson
