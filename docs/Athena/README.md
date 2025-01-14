# Athena Integration with AWS and Carbonix Athena Handler Library

## Overview
This document explains how to work with Amazon Athena in AWS and how to test queries using the `carbonix-aws-libs` Athena Handler (`athena_handler.py`). It provides details about the Athena table format, the benefits of using Parquet files for storage, and includes example queries and code usage.

---

## How Athena Works
Amazon Athena is an interactive query service that allows you to analyze data directly in Amazon S3 using SQL. It integrates seamlessly with AWS Glue to define the schema and partition structure of the data.The S3 bucket "carbonix-logs-telemetry-data-pool" containing the data is linked to the Athena table, which allows you to run SQL queries on the data. Currently, we are not using AWS Glue for schema definition, and pushing the partitions manually.

### Key Features:
- Supports standard SQL for querying.
- Optimized for Parquet file format to save storage and improve query performance.
- Allows table partitioning for efficient data retrieval.
- Cost of AWS Athena is based on the amount of data scanned. It is currently **$5 per TB of data scanned**. The minimum data scan is **10 MB per query which costs $0.00005**. 

---

## Testing Athena Queries
You can test Athena queries in two ways:

### 1. **Using the AWS Console**
- Navigate to the **Athena** service in the AWS Management Console.
- Select your database and table.
- Run SQL queries directly in the Athena Query Editor.

### 2. **Using `athena_handler.py`**
The `carbonix-aws-libs` library includes an Athena handler (`athena_handler.py`) that provides a Python-based interface for executing queries.
For this to work, you need to have the AWS CLI installed and configured with the necessary permissions. Ensure the necessary Python libraries are installed by running `pip install -r requirements.txt`. Also, the .env file should be present in the root directory with the nessary environment variables AWS_REGION, DB_SECRET_NAME, ATHENA_DATABASE, and ATHENA_OUTPUT_LOCATION.
To install the library, run.
```bash
pip install git+https://github.com/CarbonixUAV/carbonix-aws-libs.git
```

---

## Table Format
The `carbonix_logs_telemetry_data_pool` table is partitioned and has the following schema:

| Column Name   | Data Type   | Description                      |
|---------------|-------------|----------------------------------|
| `loguid`      | `string`    | Unique identifier for the log    |
| `messagetype` | `string`    | Type of message in the log       |
| `instance`    | `string`    | Log instance identifier          |
| `keyname`     | `string`    | Key name in the log              |
| `timestamp`   | `bigint`    | Log timestamp in milliseconds    |
| `linenumber`  | `bigint`    | Line number in the log (help grouping a single message)          |
| `value`       | `float`     | Numerical value                  |
| `stringvalue` | `string`    | String representation of value   |
| `binaryvalue` | `binary`    | Binary value                     |

### Partition Columns:
- `loguid`
- `messagetype`
- `instance`
- `keyname`

### Benefits of Partitioning:
- Reduces query scope to only the relevant data.
- Improves query performance by scanning fewer files.

## Parquet File Savings
We store the data in Parquet format due to the following advantages:
- **Compression**: Parquet uses columnar storage, resulting in significant storage savings.
- **Query Performance**: Only the required columns are read during queries.
- **Cost-Effectiveness**: Reduces data scanning, lowering query costs.
---

## Example Athena Queries

### Add a New Partition
```sql
ALTER TABLE carbonix_logs_telemetry_data_pool
ADD PARTITION (
    loguid='log123',
    messagetype='STATUSTEXT',
    instance='1',
    keyname='severity'
)
LOCATION 's3://carbonix-logs-telemetry-data-pool/loguid=log123/messagetype=STATUSTEXT/instance=1/keyname=severity';
```

### Check if a Log Exists
```sql
SELECT loguid
FROM carbonix_logs_telemetry_data_pool
WHERE loguid = 'log123'
LIMIT 1;
```

### Join and Filter Logs
```sql
SELECT DISTINCT
    t1.value AS severity_level,
    t2.stringvalue AS text_value
FROM carbonix_logs_telemetry_data_pool t1
JOIN carbonix_logs_telemetry_data_pool t2
  ON t1.LogUID = t2.LogUID
  AND t1.MessageType = t2.MessageType
  AND t1.Instance = t2.Instance
  AND t1.Timestamp = t2.Timestamp
  AND t1.linenumber = t2.linenumber
WHERE t1.messagetype = 'STATUSTEXT'
  AND t1.instance = '1'
  AND t1.keyname = 'severity'
  AND t1.value < 2
  AND t2.keyname = 'text';
```

---

## Using the Athena Handler Library
The `athena_handler.py` library provides several utility functions:

### Key Functions
- **`execute_query(query: str)`**: Executes a query and returns the query execution ID.
- **`get_query_status(query_execution_id: str)`**: Checks the status of a query (e.g., `SUCCEEDED`, `FAILED`).
- **`wait_for_query_to_complete(query_execution_id: str, delay: int = 5)`**: Waits until a query is completed.
- **`get_query_results(query_execution_id: str)`**: Retrieves the query results.
- **`add_partitions(partition_list: list, s3_bucket_name: str)`**: Adds partitions to the Athena table.

- There are more readymade functions available in the library. Refer to the library for more details. So the no need to write queries manually.

### Example Code for Check if Log Exists Using internal Function
```python
from carbonix_aws_libs.athena_handler import AthenaHandler

athena_handler = AthenaHandler(
        database='telemetry_pool_v5.carbonix_logs_telemetry_data_pool',
        output_location='s3://carbonix-athnea-result/',
        region_name='ap-southeast-2')
loguid = '0b5a963e37fc0e0f43cd0730582069715161b2ccffe5c07698a9c8a593fa72b9'
if athena_handler.check_loguid_exists(loguid):
    print(f"The loguid '{loguid}' exists in Athena table.")
    exit(0)
else:
    print(f"The loguid '{loguid}' does not exist in Athena table.")
    exit(1)
```

### Example Code for Checking LogUID exist Making your own Query
```python
from carbonix_aws_libs.athena_handler import AthenaHandler
athena_handler = AthenaHandler(
        database='telemetry_pool_v5.carbonix_logs_telemetry_data_pool',
        output_location='s3://carbonix-athnea-result/',
        region_name='ap-southeast-2')
query = f"SELECT loguid FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool WHERE loguid = '{loguid}' LIMIT 1;"

query_execution_id = athena_handler.execute_query(query)
if query_execution_id:
    query_completed = athena_handler.wait_for_query_to_complete(query_execution_id)
    if query_completed:
        results = athena_handler.get_query_results(query_execution_id)
        if results:
            print(f"The loguid '{loguid}' exists in Athena table.")
            exit(0)
        else:
            print(f"The loguid '{loguid}' does not exist in Athena table.")
            exit(1)
    else:
        print("Query execution failed.")
        exit(1)
```

---

## Notes
- Monitor query costs, as Athena charges based on the amount of data scanned.
