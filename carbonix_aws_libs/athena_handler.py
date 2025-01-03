import logging
import time
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class AthenaHandler:
    def __init__(self, database: str, output_location: Optional[str] = None):
        self.athena_client = boto3.client('athena')
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
            logger.debug(f"Started query execution: {query_execution_id}")
            return query_execution_id
        except ClientError as e:
            logger.debug(f"Error executing query: {e}")
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
            logger.debug(f"Error getting query status: {e}")
            return None

    def wait_for_query_to_complete(self, query_execution_id: str, delay: int = 5) -> bool:
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

    def get_query_results(self, query_execution_id: str) -> Optional[Dict[str, Any]]:
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
            logger.debug(f"Error getting query results: {e}")
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
        if query_execution_id and self.wait_for_query_to_complete(query_execution_id):
            results = self.get_query_results(query_execution_id)
            if results and 'ResultSet' in results and 'Rows' in results['ResultSet']:
                # The first row is the header
                return len(results['ResultSet']['Rows']) > 1
        return False
    
    def get_boot_time(self, loguid: str, file_type:str) -> Optional[Dict[str, Any]]:
        """
        get boot time from the Athena table based on the loguid.

        :param loguid: The loguid to get boot time for.
        :file_type: The log type to get data for.
        :return: The boot time or None if an error occurs.
        """
        """
        SELECT loguid, timestamp
        FROM telemetry_pool_v4.carbonix_logs_telemetry_data_pool
        WHERE loguid = 'fd3be2ec0c7405080e79c85ee3a7edf38ae0100c8cd8ea04c99ac6a10c990c93'
        AND (MessageType = 'FMT')
        AND (KeyName = 'Type')
        ORDER BY timestamp ASC
        LIMIT 1;
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
        if query_execution_id and self.wait_for_query_to_complete(query_execution_id):
            results = self.get_query_results(query_execution_id)
            if results and 'ResultSet' in results and 'Rows' in results['ResultSet']:
                return results['ResultSet']['Rows'][1]['Data'][1]['VarCharValue']
        return None        

