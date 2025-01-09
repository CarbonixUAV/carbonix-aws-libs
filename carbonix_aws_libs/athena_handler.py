import logging
import time
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AthenaHandler:
    def __init__(self, database: str, output_location: Optional[str] = None,
                 region_name: Optional[str] = 'ap-southeast-2'):
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.database = database
        self.output_location = output_location

    def execute_query(self, query: str) -> Optional[str]:
        """
        Execute an Athena query.

        :param query: The SQL query to execute.
        :return: The query execution ID or None if an error occurs.
        """
        try:
            params = {
                'QueryString': query,
                'QueryExecutionContext': {'Database': self.database}
            }
            if self.output_location:
                params['ResultConfiguration'] = {
                    'OutputLocation': self.output_location}
            response = self.athena_client.start_query_execution(**params)
            query_execution_id = response['QueryExecutionId']
            logger.debug(f"{query_execution_id}")
            return query_execution_id
        except ClientError as e:
            logger.error(f"{e}")
            return None

    def get_query_status(self, query_execution_id: str) -> Optional[str]:
        """
        Get the status of an Athena query.

        :param query_execution_id: The ID of the query execution.
        :return: The status of the query or None if an error occurs.
        """
        try:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            logger.debug(f"Query {query_execution_id} status: {status}")
            return status
        except ClientError as e:
            logger.error(f"Error getting query status: {e}")
            return None

    def wait_for_query_to_complete(self, query_execution_id: str,
                                   delay: int = 5) -> bool:
        """
        Wait for an Athena query to complete.

        :param query_execution_id: The ID of the query execution.
        :param delay: The delay between status checks in seconds.
        :return: True if the query completed successfully, False otherwise.
        """
        while True:
            status = self.get_query_status(query_execution_id)
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status == 'SUCCEEDED'
            time.sleep(delay)

    def get_query_results(self,
                          query_execution_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve the results of an Athena query.

        :param query_execution_id: The ID of the query execution.
        :return: The query results or None if an error occurs.
        """
        try:
            response = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id)
            return response
        except ClientError as e:
            logger.error(f"Error getting query results: {e}")
            return None

    def check_loguid_exists(self, loguid: str) -> bool:
        """
        Check if a specific loguid exists in the Athena table.

        :param loguid: The loguid to check.
        :return: True if the loguid exists, False otherwise.
        """
        query = f"""
        SELECT loguid
        FROM {self.database}
        WHERE loguid = '{loguid}'
        LIMIT 1;
        """
        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return False
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if results and 'ResultSet' in results and (
                    'Rows' in results['ResultSet']):
                # The first row is the header
                return len(results['ResultSet']['Rows']) > 1
        return False

    def get_boot_time(self, loguid: str,
                      file_type: str) -> Optional[Dict[str, Any]]:
        """
        get boot time from the Athena table based on the loguid.

        :param loguid: The loguid to get boot time for.
        :file_type: The log type to get data for.
        :return: The boot time or None if an error occurs.
        """

        if file_type == ".BIN":
            query = f"""
            SELECT loguid, timestamp
            FROM {self.database}
            WHERE loguid = '{loguid}'
            AND (MessageType = 'FMT')
            AND (KeyName = 'Type')
            ORDER BY timestamp ASC
            LIMIT 1;
            """
        elif file_type == ".TLOG":
            query = f"""
            SELECT loguid, timestamp
            FROM {self.database}
            WHERE loguid = '{loguid}'
            AND (MessageType = 'HEARTBEAT')
            AND (KeyName = 'type')
            ORDER BY timestamp ASC
            LIMIT 1;
            """
        else:
            return None

        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return None
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if results and 'ResultSet' in results and (
                    'Rows' in results['ResultSet']):
                first_row = results['ResultSet']['Rows'][1]
                value = first_row['Data'][1]['VarCharValue']
                return value
        return None

    def get_add_partition_query(self, partition_list: list,
                                s3_bucket_name: str) -> str:
        """
        Create an ALTER TABLE query to add partitions to an Athena table.

        :param partition_list: A list of partition values.
        :param s3_bucket_name: The name of the S3 bucket.
        :return: The ALTER TABLE query.
        """
        partitions = []
        # create the ALTER TABLE query
        query = f"ALTER TABLE {self.database} ADD\n"

        for folder in partition_list:
            try:
                # Normalize folder path to use forward slashes
                folder = folder.replace("\\", "/").lstrip("/").rstrip("\n")
                parts = folder.split("/")
                loguid = parts[0].split("=")[1]
                messagetype = parts[1].split("=")[1]
                instance = parts[2].split("=")[1]
                keyname = parts[3].split("=")[1]
                # Construct the PARTITION clause
                partition = (f"PARTITION (loguid='{loguid}', "
                             f"messagetype='{messagetype}', "
                             f"instance='{instance}', keyname='{keyname}')\n"
                             f"LOCATION 's3://{s3_bucket_name}/{folder}'")
                partitions.append(partition)
            except IndexError:
                logger.error(f"Error parsing folder path: {folder}")
                continue

        if not partitions:
            return None
        query += "\n".join(partitions) + ";"
        return query

    def add_partitions(self, partition_list: list,
                       s3_bucket_name: str) -> bool:
        """
        Add partitions to an Athena table.

        :param partition_list: A list of partition values.
        :param s3_bucket_name: The name of the S3 bucket.
        :return: True if the partitions were added or False.
        """
        query = self.get_add_partition_query(partition_list, s3_bucket_name)
        if not query:
            return False
        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return False
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        return query_completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    for lib in ('boto3', 'botocore', 'urllib3', 'pandas',
                'pyarrow', 'pymavlink', 's3transfer'):
        # Change from DEBUG to WARNING
        logging.getLogger(lib).setLevel(logging.WARNING)

    athena_handler = AthenaHandler(
        database='telemetry_pool_v5.carbonix_logs_telemetry_data_pool',
        output_location='s3://carbonix-athnea-result/',
        region_name='ap-southeast-2')
    loguid = '4b0cb7be12061b2289756c749a8c0744be875beba82ab0fe94cc3d5c9f68ee8f'
    if athena_handler.check_loguid_exists(loguid):
        logger.info(f"The loguid '{loguid}' exists in Athena table.")
    else:
        logger.info(f"The loguid '{loguid}' does not exist in Athena table.")
