import logging
import time
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import ClientError
from typing import List

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

    def get_fc_firmware_query(self, loguid: str) -> str:
        """
        Generate SQL query to retrieve firmware information from telemetry.

        :param loguid: SHA256 hash for the log entry
        :return: SQL query string
        """
        return f"""
        WITH RankedLogs AS (
            SELECT 
                loguid, messagetype, instance, keyname, stringvalue,
                ROW_NUMBER() OVER (PARTITION BY loguid ORDER BY loguid) AS rn
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
              AND (
                (messagetype = 'MSG' AND instance = '0' AND keyname = 'Message' 
                 AND (LOWER(stringvalue) LIKE '%ardupilot%' OR LOWER(stringvalue) LIKE '%carbopilot%' OR LOWER(stringvalue) LIKE '%cxpilot%'))
                OR 
                (messagetype = 'STATUSTEXT' AND instance = '1' AND keyname = 'text'
                 AND (LOWER(stringvalue) LIKE '%ardupilot%' OR LOWER(stringvalue) LIKE '%carbopilot%' OR LOWER(stringvalue) LIKE '%cxpilot%'))
              )
        )
        SELECT loguid, messagetype, instance, keyname, stringvalue
        FROM RankedLogs
        WHERE rn = 1
        ORDER BY loguid;
        """

    def get_fc_firmware(self, loguid: str) -> Optional[str]:
        """
        Retrieve the firmware information from the telemetry data.

        :param loguid: The loguid to get firmware information for.
        :return: The firmware information or None if an error occurs.
        """
        query = self.get_fc_firmware_query(loguid)
        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return None
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if (results and 'ResultSet' in results and 'Rows' in
                    results['ResultSet']):
                rows = results['ResultSet']['Rows']
                if (len(rows) > 1 and 'Data' in rows[1] and
                        len(rows[1]['Data']) > 4):
                    return rows[1]['Data'][4]['VarCharValue']
        return None

    def get_unique_instance_query(self, loguid: str,
                                  message_type: str) -> str:
        """
        Generate SQL query to retrieve the number of unique instances
        for a specified message type.

        :param loguid: SHA256 hash for the log entry
        :param message_type: The type of message to filter by
        :return: SQL query string
        """
        return f"""
        SELECT DISTINCT instance AS instances
        FROM
            telemetry_pool_v5.carbonix_logs_telemetry_data_pool
        WHERE 
            loguid = '{loguid}'
            AND messagetype = '{message_type}'
        ORDER BY instances;
        """

    def get_unique_instance(self, loguid: str,
                            message_type: str) -> Optional[list]:
        """
        Retrieve the number of unique instances for a specified message type.

        :param loguid: The loguid to get unique instances for.
        :param message_type: The type of message to filter by.
        :return: The number of unique instances or None if an error occurs.
        """
        query = self.get_unique_instance_query(loguid, message_type)
        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return None
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if (results and 'ResultSet' in results and 'Rows'
                    in results['ResultSet']):
                rows = results['ResultSet']['Rows']
                instances = []
                for row in rows[1:]:
                    if 'Data' in row and len(row['Data']) > 0:
                        instances.append(row['Data'][0]['VarCharValue'])
                return instances
        return None

    def get_binlog_flight_query(self, loguid, start_time):
        return f"""
        WITH Takeoff AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                from_unixtime(cast(timestamp/1000 AS bigint)) AS takeoff_timestamp_str,
                value AS base_mode_value
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'STAT'
            AND instance = '0' -- String comparison
            AND keyname = 'Armed'
            AND value = 1
            AND timestamp >= cast('{start_time}' AS bigint)
            AND EXISTS (
                SELECT 1
                FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool AS sub
                WHERE sub.loguid = '{loguid}'
                    AND sub.messagetype = 'STAT'
                    AND sub.instance = '0'
                    AND sub.keyname = 'isFlying'
                    AND sub.value = 1
                    AND sub.timestamp = telemetry_pool_v5.carbonix_logs_telemetry_data_pool.timestamp
            )
            ORDER BY timestamp ASC
            LIMIT 1
        ),
        Landing AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                from_unixtime(cast(timestamp/1000 AS bigint)) AS landing_timestamp_str,
                value AS base_mode_value
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'STAT'
            AND instance = '0' -- String comparison
            AND keyname = 'Armed'
            AND value = 0
            AND timestamp > (SELECT takeoff_timestamp FROM Takeoff)
            AND EXISTS (
                SELECT 1
                FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool AS sub
                WHERE sub.loguid = '{loguid}'
                    AND sub.messagetype = 'STAT'
                    AND sub.instance = '0'
                    AND sub.keyname = 'isFlying'
                    AND sub.value = 0
                    AND sub.timestamp = telemetry_pool_v5.carbonix_logs_telemetry_data_pool.timestamp
            )
            ORDER BY timestamp ASC
            LIMIT 1
        ),
        TakeoffLat AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                value AS takeoff_lat
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GPS'
            AND instance = '0'
            AND keyname = 'Lat'
            ORDER BY ABS(timestamp - (SELECT takeoff_timestamp FROM Takeoff)) ASC
            LIMIT 1
        ),
        TakeoffLong AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                value AS takeoff_long
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GPS'
            AND instance = '0'
            AND keyname = 'Lng'
            ORDER BY ABS(timestamp - (SELECT takeoff_timestamp FROM Takeoff)) ASC
            LIMIT 1
        ),
        LandingLat AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                value AS landing_lat
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GPS'
            AND instance = '0'
            AND keyname = 'Lat'
            ORDER BY ABS(timestamp - (SELECT landing_timestamp FROM Landing)) ASC
            LIMIT 1
        ),
        LandingLong AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                value AS landing_long
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GPS'
            AND instance = '0'
            AND keyname = 'Lng'
            ORDER BY ABS(timestamp - (SELECT landing_timestamp FROM Landing)) ASC
            LIMIT 1
        ),
        PilotGSO AS (
            SELECT loguid, 
                timestamp, 
                stringvalue AS message
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'MSG'
            AND instance = '0' -- String comparison
            AND keyname = 'Message'
            AND timestamp >= cast('{start_time}' AS bigint)
            AND timestamp <= (SELECT landing_timestamp FROM Landing)
            AND (stringvalue LIKE '%PIC:%' OR stringvalue LIKE '%GSO:%')
            ORDER BY timestamp ASC
            LIMIT 2
        )
        SELECT 
            t.takeoff_timestamp,
            t.takeoff_timestamp_str,
            tl.takeoff_lat,
            tlon.takeoff_long,
            pg.message AS pilot,
            pg2.message AS gso,
            l.landing_timestamp,
            l.landing_timestamp_str,
            ll.landing_lat,
            llon.landing_long,
            (l.landing_timestamp - t.takeoff_timestamp) / 1000 AS total_flight_time
        FROM Takeoff t
        JOIN Landing l ON t.loguid = l.loguid
        JOIN TakeoffLat tl ON t.loguid = tl.loguid
        JOIN TakeoffLong tlon ON t.loguid = tlon.loguid
        JOIN LandingLat ll ON l.loguid = ll.loguid
        JOIN LandingLong llon ON l.loguid = llon.loguid
        LEFT JOIN PilotGSO pg ON t.loguid = pg.loguid AND pg.message LIKE '%PIC:%'
        LEFT JOIN PilotGSO pg2 ON t.loguid = pg2.loguid AND pg2.message LIKE '%GSO:%';
        """

    def tlog_flight_data_query(self, loguid, start_time):
        return f"""
        WITH Takeoff AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                from_unixtime(cast(timestamp/1000 AS bigint)) AS takeoff_timestamp_str,
                value AS base_mode_value
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'HEARTBEAT'
            AND instance = '1'
            AND keyname = 'base_mode'
            AND value >= 128
            AND timestamp >= cast('{start_time}' AS bigint)
            AND EXISTS (
                SELECT 1
                FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool AS sub
                WHERE sub.loguid = '{loguid}'
                    AND sub.messagetype = 'VFR_HUD'
                    AND sub.instance = '1'
                    AND sub.keyname = 'throttle'
                    AND sub.value >= 5
                    AND ABS(sub.timestamp - telemetry_pool_v5.carbonix_logs_telemetry_data_pool.timestamp) <= 1000
            )
            AND EXISTS (
                SELECT 1
                FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool AS sub
                WHERE sub.loguid = '{loguid}'
                    AND sub.messagetype = 'VFR_HUD'
                    AND sub.instance = '1'
                    AND sub.keyname = 'groundspeed'
                    AND sub.value >= 3
                    AND ABS(sub.timestamp - telemetry_pool_v5.carbonix_logs_telemetry_data_pool.timestamp) <= 1000
            )
            ORDER BY timestamp ASC
            LIMIT 1
        ),
        Landing AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                from_unixtime(cast(timestamp/1000 AS bigint)) AS landing_timestamp_str,
                value AS base_mode_value
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'HEARTBEAT'
            AND instance = '1'
            AND keyname = 'base_mode'
            AND value < 128
            AND timestamp > (SELECT takeoff_timestamp FROM Takeoff)
            ORDER BY timestamp ASC
            LIMIT 1
        ),
        TakeoffLat AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                value AS takeoff_lat
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GLOBAL_POSITION_INT'
            AND instance = '1'
            AND keyname = 'lat'
            ORDER BY ABS(timestamp - (SELECT takeoff_timestamp FROM Takeoff)) ASC
            LIMIT 1
        ),
        TakeoffLong AS (
            SELECT loguid, 
                timestamp AS takeoff_timestamp, 
                value AS takeoff_long
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GLOBAL_POSITION_INT'
            AND instance = '1'
            AND keyname = 'lon'
            ORDER BY ABS(timestamp - (SELECT takeoff_timestamp FROM Takeoff)) ASC
            LIMIT 1
        ),
        LandingLat AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                value AS landing_lat
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GLOBAL_POSITION_INT'
            AND instance = '1'
            AND keyname = 'lat'
            ORDER BY ABS(timestamp - (SELECT landing_timestamp FROM Landing)) ASC
            LIMIT 1
        ),
        LandingLong AS (
            SELECT loguid, 
                timestamp AS landing_timestamp, 
                value AS landing_long
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'GLOBAL_POSITION_INT'
            AND instance = '1'
            AND keyname = 'lon'
            ORDER BY ABS(timestamp - (SELECT landing_timestamp FROM Landing)) ASC
            LIMIT 1
        ),
        PilotGSO AS (
            SELECT loguid, 
                timestamp, 
                stringvalue AS message
            FROM telemetry_pool_v5.carbonix_logs_telemetry_data_pool
            WHERE loguid = '{loguid}'
            AND messagetype = 'STATUSTEXT'
            AND keyname = 'text'
            AND timestamp >= cast('{start_time}' AS bigint)
            AND timestamp <= (SELECT landing_timestamp FROM Landing)
            AND (stringvalue LIKE '%PIC:%' OR stringvalue LIKE '%GSO:%')
            ORDER BY timestamp ASC
            LIMIT 2
        )
        SELECT 
            t.takeoff_timestamp,
            t.takeoff_timestamp_str,
            tl.takeoff_lat,
            tlon.takeoff_long,
            pg.message AS pilot,
            pg2.message AS gso,
            l.landing_timestamp,
            l.landing_timestamp_str,
            ll.landing_lat,
            llon.landing_long,
            (l.landing_timestamp - t.takeoff_timestamp) / 1000 AS total_flight_time
        FROM Takeoff t
        JOIN Landing l ON t.loguid = l.loguid
        JOIN TakeoffLat tl ON t.loguid = tl.loguid
        JOIN TakeoffLong tlon ON t.loguid = tlon.loguid
        JOIN LandingLat ll ON l.loguid = ll.loguid
        JOIN LandingLong llon ON l.loguid = llon.loguid
        LEFT JOIN PilotGSO pg ON t.loguid = pg.loguid AND pg.message LIKE '%PIC:%'
        LEFT JOIN PilotGSO pg2 ON t.loguid = pg2.loguid AND pg2.message LIKE '%GSO:%';
        """

    def get_flight_data(self, loguid: str, start_time: str,
                        file_type: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve the flight data from the telemetry data.

        :param loguid: The loguid to get flight data for.
        :param start_time: The start time of the flight.
        :param file_type: The type of file to get data for.
        :return: The flight data or None if an error occurs.
        """
        if file_type.lower() in (".bin", "bin"):
            query = self.get_binlog_flight_query(loguid, start_time)
        elif file_type.lower() in (".tlog", "tlog"):
            query = self.tlog_flight_data_query(loguid, start_time)
        else:
            return None

        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return None
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if (results and 'ResultSet' in results and 'Rows' in
                    results['ResultSet']):
                rows = results['ResultSet']['Rows']
                if (len(rows) > 1 and 'Data' in rows[1] and
                        len(rows[1]['Data']) > 10):
                    return {
                        'TakeoffTimestamp': rows[1]['Data'][0]['VarCharValue'],
                        'TakeoffTimestampStr': rows[1]['Data'][1]['VarCharValue'],
                        'TakeoffLat': rows[1]['Data'][2]['VarCharValue'],
                        'TakeoffLong': rows[1]['Data'][3]['VarCharValue'],
                        'Pilot': rows[1]['Data'][4]['VarCharValue'],
                        'GSO': rows[1]['Data'][5]['VarCharValue'],
                        'LandingTimestamp': rows[1]['Data'][6]['VarCharValue'],
                        'LandingTimestampStr': rows[1]['Data'][7]['VarCharValue'],
                        'LandingLat': rows[1]['Data'][8]['VarCharValue'],
                        'LandingLong': rows[1]['Data'][9]['VarCharValue'],
                        'TotalFlightTime': rows[1]['Data'][10]['VarCharValue']
                    }
        return None

    def get_value_stats_query(self, loguid: str, message_type: str,
                              instance: int, keynames: List[str],
                              start_time: int, stop_time: int) -> str:
        """
        Generate SQL query to retrieve minimum, maximum, and average values 
        for a specified loguid, messageType,
        multiple keynames, and instance within a given time range.

        :param loguid: The unique identifier of the log entry
        :param message_type: The type of message to filter by
        :param keynames: List of key names to filter by
        :param instance: The instance identifier to filter by
        :param start_time: Start timestamp of the range
        :param stop_time: Stop timestamp of the range
        :return: SQL query string
        """
        keyname_list = ', '.join([f"'{keyname}'" for keyname in keynames])
        return f"""
        SELECT
            loguid,
            messagetype,
            keyname,
            instance,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            AVG(value) AS avg_value
        FROM
            telemetry_pool_v5.carbonix_logs_telemetry_data_pool
        WHERE
            loguid = '{loguid}'
            AND messagetype = '{message_type}'
            AND keyname IN ({keyname_list})
            AND CAST(instance AS integer) = {instance}
            AND timestamp >= {start_time}
            AND timestamp <= {stop_time}
        GROUP BY
            loguid, messagetype, keyname, instance;
        """

    def get_value_stats(self, loguid: str, message_type: str,
                        instance: int, keynames: List[str],
                        start_time: int, stop_time: int
                        ) -> Optional[Dict[str, Dict[str, float]]]:
        """
        Execute a query to retrieve minimum, maximum, and average values
        for a specified loguid, messageType,
        multiple keynames, and instance within a given time range.

        :param loguid: The unique identifier of the log entry
        :param message_type: The type of message to filter by
        :param keynames: List of key names to filter by
        :param instance: The instance identifier to filter by
        :param start_time: Start timestamp of the range
        :param stop_time: Stop timestamp of the range
        :return: Dictionary containing min, max, and avg values for each
        keyname, or None if not found
        """
        query = self.get_value_stats_query(loguid, message_type, instance,
                                           keynames, start_time, stop_time)
        query_execution_id = self.execute_query(query)
        if not query_execution_id:
            return None
        query_completed = self.wait_for_query_to_complete(query_execution_id)
        if query_completed:
            results = self.get_query_results(query_execution_id)
            if (results and 'ResultSet' in results and 'Rows' in
                    results['ResultSet']):
                rows = results['ResultSet']['Rows']
                if len(rows) > 1:
                    stats = {}
                    for row in rows[1:]:
                        keyname = row['Data'][2]['VarCharValue']
                        stats[keyname] = {
                            'min': float(row['Data'][4]['VarCharValue']),
                            'max': float(row['Data'][5]['VarCharValue']),
                            'avg': float(row['Data'][6]['VarCharValue'])
                        }
                    return stats
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    for lib in ('boto3', 'botocore', 'urllib3', 'pandas',
                'pyarrow', 'pymavlink', 's3transfer'):
        # Change from DEBUG to WARNING
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Example Bin File data which you can fetch from the lambda or RDS.
    binlog = {
        "SHA256Hash": "7af135f86f7f0eaeb7a39584a21fc332fdc9804d97dc86d63ea790c40a209759",
        "AircraftID": 30,
        "LogType": "bin",
        "LogFileName": "O10_57_20241021-225047.bin",
        "LogFileSize": 481816576,
        "StartTime": "2024-10-21 22:50:47",
        "PilotComments": "",
        "S3FileLocation": "s3://carbonix-all-logs/O10_57_20241021-225047.bin",
        "AircraftName": "O10",
        "CubeID": "00440029 34305109 37373930",
        "BootNumber": 57,
        "LogUID": 122,
        "FlightData": {
            "TakeoffTimestamp": "1729548328901",
            "TakeoffTimestampStr": "2024-10-21 22:05:28.901000",
            "TakeoffLat": "-33.66723",
            "TakeoffLong": "150.85442",
            "Pilot": "GCS:PIC:Isaac Straatemeier",
            "GSO": "GCS:GSO:Lachlan Conn",
            "LandingTimestamp": "1729551127702",
            "LandingTimestampStr": "2024-10-21 22:52:07.702000",
            "LandingLat": "-33.66724",
            "LandingLong": "150.8544",
            "TotalFlightTime": "2798",
            "FlightID": "O10_2024-10-21_22_05_1508_3366",
            "LogUID": 122,
            "PilotUID": 3,
            "GSOUID": 1,
            "BootTimestamp": "1729547775780",
            "BootTimestampStr": "2024-10-21 21:56:15.780000",
            "Version": "1.0",
            "FlightUID": 54
        }
    }

    event = binlog
    loguid = event.get('SHA256Hash')
    log_type = event.get('LogType')
    flight_data = event.get('FlightData')

    athena_handler = AthenaHandler(
        database='telemetry_pool_v5.carbonix_logs_telemetry_data_pool',
        output_location='s3://carbonix-athnea-result/',
        region_name='ap-southeast-2')

    if athena_handler.check_loguid_exists(loguid):
        logger.info(f"The loguid '{loguid}' exists.")
    else:
        logger.info(f"The loguid '{loguid}' does not exist.")
        exit()
    boot_time = athena_handler.get_boot_time(loguid, ".BIN")
    logger.info(f"Boot time for loguid '{loguid}': {boot_time}")

    fc_firmware = athena_handler.get_fc_firmware(loguid)
    logger.info(f"Firmware information for loguid "
                f"'{loguid}': {fc_firmware}")

    bat_instance = athena_handler.get_unique_instance(loguid, "BAT")
    logger.info(f"Unique instances for loguid "
                f"'{loguid}': {bat_instance}")
    if bat_instance:
        for instance in bat_instance:
            keynames = ['Volt', 'Curr', 'VoltR']
            start_time = flight_data.get('TakeoffTimestamp')
            stop_time = flight_data.get('LandingTimestamp')
            stats = athena_handler.get_value_stats(
                loguid, "BAT", int(instance), keynames, start_time, stop_time)
            logger.info(f"Stats for instance {instance}: {stats}")
