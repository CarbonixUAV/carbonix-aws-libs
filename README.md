# Carbonix AWS Libs

**Carbonix AWS Libs** is a Python library collection designed to integrate with AWS tools like Athena, Glue, and Aurora for supporting the Carbonix log analysis system.

## Features

- **AthenaHandler**: A utility for querying AWS Athena databases and managing query results.
- **GlueCrawlerHandler**: A utility for managing AWS Glue Crawlers for log data processing.
- **AuroraHandler**: A utility for interacting with AWS RDS Aurora for log and flight data storage.
- **S3Handler**: A utility for managing AWS S3 buckets and objects.

## Installation

Install the library directly from GitHub:

```bash
pip install git+https://github.com/CarbonixUAV/carbonix-aws-libs.git
```

## Usage

### Import Libraries

```python
from carbonix_aws_libs.athena_handler import AthenaHandler
from carbonix_aws_libs.glue_handler import GlueCrawlerHandler
from carbonix_aws_libs.aurora_handler import AuroraHandler
from carbonix_aws_libs.s3_handler import S3Handler
```

### Example Usage

#### Querying Athena

```python
athena_handler = AthenaHandler(
    database="my_database",
    output_location="s3://my-bucket/"
)
query_id = athena_handler.execute_query("SELECT * FROM logs LIMIT 10;")
if query_id and athena_handler.wait_for_query_to_complete(query_id):
    results = athena_handler.get_query_results(query_id)
    print(results)
```

#### Triggering a Glue Crawler

```python
glue_handler = GlueCrawlerHandler(crawler_name="my_crawler")
if not glue_handler.is_crawler_running():
    glue_handler.start_crawler()
    print("Crawler started.")
```

#### Connecting to Aurora

```python
aurora_handler = AuroraHandler({
    'host': 'my-db-host',
    'username': 'admin',
    'password': 'my-password',
    'dbname': 'my_database',
    'port': 3306,
    'region': 'us-east-1',
    'secret_name': 'my-secret-name'
})
if aurora_handler.init_state:
    results = aurora_handler.execute_query("SELECT * FROM FlightTable;")
    print(results)
```

#### S3 Bucket Operations

```python
s3_handler = S3Handler(bucket_name="my-bucket")
s3_handler.upload_file("my-file.txt", "path/to/my-file.txt")
s3_handler.download_file("path/to/my-file.txt", "my-file.txt")
```
