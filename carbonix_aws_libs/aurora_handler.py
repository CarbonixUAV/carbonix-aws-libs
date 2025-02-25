import logging
import pymysql
import json
import boto3
from typing import List, Dict, Optional, Tuple, Any
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AuroraHandler:
    def __init__(self, db_credentials: Dict[str, str]):
        """
        Initialize the AuroraHandler instance with database credentials 
        and establish a connection.
        """
        self.db_credentials = db_credentials
        self.connection = None
        self.init_state = False
        self.retrieve_db_credentials()
        self.init_state = self.connect()

    def retrieve_db_credentials(self) -> None:
        """Retrieve and store database credentials from AWS Secrets Manager."""
        client = boto3.session.Session().client(
            service_name='secretsmanager',
            region_name=self.db_credentials['region']
        )
        try:
            response = client.get_secret_value(
                SecretId=self.db_credentials['secret_name'])
            secret = json.loads(response['SecretString'])
            self.db_credentials['password'] = secret['password']
            logger.debug("Secret retrieved successfully from Secrets Manager.")
        except Exception as e:
            logger.error(f"Error retrieving secret: {e}")

    def reconnect(self) -> bool:
        """Reconnect to the database."""
        logger.debug("Reconnecting to the database...")
        if self.connection:
            self.close_connection()
        self.retrieve_db_credentials()
        return self.connect()

    def connected(self) -> bool:
        """Check if the handler is connected to the database."""
        return self.connection

    def connect(self) -> bool:
        """Establish a connection to the RDS database."""
        try:
            self.connection = pymysql.connect(
                host=self.db_credentials['host'],
                user=self.db_credentials['username'],
                password=self.db_credentials['password'],
                db=self.db_credentials['dbname'],
                port=int(self.db_credentials['port'])
            )
            logger.info("Connected to the database successfully.")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False

    def close_connection(self) -> None:
        """Close the database connection if open."""
        if self.connection:
            try:
                self.connection.close()
                logger.debug("Database connection closed.")
            except pymysql.MySQLError as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.connection = None

    def execute_query(self, query: str, params: Optional[Tuple] = None,
                      fetchone: bool = False):
        """Execute a query and return results if applicable."""
        if not self.connection and not self.connect():
            logger.error("Database connection failed.")
            return None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone() if fetchone else cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None

    def execute_insert_or_update(self, query: str, params: Tuple) -> bool:
        """Execute an insert or update query and commit the transaction."""
        if not self.connection and not self.connect():
            logger.error("Database connection failed.")
            return False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Transaction error: {e}")
            self.connection.rollback()
            return False

    def get_uid_by_column_str(self, table: str, column_name: str,
                              stringValue: str) -> Optional[str]:
        """Retrieve UID from specified table based on the TypeName."""
        query = f"SELECT UID FROM {table} WHERE {column_name} = %s"
        result = self.execute_query(query, (stringValue,), fetchone=True)
        if result:
            logger.debug(f"{stringValue} found in {table}")
            return result[0]
        logger.info(f"FAILED: {stringValue} not found in {table}")
        return None

    def insert_summary(self, data: Dict[str, Optional[str]]) -> bool:
        """Insert a new record into the SummaryTable."""
        logger.debug(f"Inserting summary data: {data}")
        columns = [key for key in data if data[key] is not None]
        values = [data[key] for key in columns]
        placeholders = ", ".join(["%s"] * len(values))
        query = f"INSERT INTO SummaryTable ({', '.join(columns)}) VALUES ({placeholders})" # noqa
        return self.execute_insert_or_update(query, tuple(values))

    def insert_error(self, data: Dict[str, Optional[str]]) -> bool:
        """Insert a new record into the ErrorTable."""
        columns = [key for key in data if data[key] is not None]
        values = [data[key] for key in columns]
        placeholders = ", ".join(["%s"] * len(values))
        query = f"INSERT INTO ErrorTable ({', '.join(columns)}) VALUES ({placeholders})" # noqa
        return self.execute_insert_or_update(query, tuple(values))

    def log_exists(self, sha256hash: str) -> bool:
        """Check if a log exists in LogTable by SHA256Hash."""
        query = "SELECT COUNT(1) FROM LogTable WHERE SHA256Hash = %s"
        result = self.execute_query(query, (sha256hash,), fetchone=True)
        return result[0] > 0 if result else False

    def insert_log(self, log_data: Dict[str, Optional[str]]) -> bool:
        """Insert a log entry into LogTable."""
        columns = [key for key in log_data if log_data[key] is not None]
        values = [log_data[key] for key in columns]
        placeholders = ", ".join(["%s"] * len(values))
        query = f"INSERT INTO LogTable ({', '.join(columns)}) VALUES ({placeholders})" # noqa
        return self.execute_insert_or_update(query, tuple(values))

    def update_telemetry_info(self, sha256hash: str, version: str,
                              path: str) -> bool:
        """Update telemetry extraction info in LogTable."""
        query = """
            UPDATE LogTable
            SET TelemExtractionVersion = %s, TelemExtractionPath = %s
            WHERE SHA256Hash = %s
        """
        return self.execute_insert_or_update(query,
                                             (version, path, sha256hash))

    def insert_to_flighttable(self,
                              flight_data: Dict[str, Optional[str]]
                              ) -> Optional[str]:
        """Insert a new flight entry into the FlightTable."""
        if not self.connection:
            self.connect()

        if not self.connection:
            return False

        try:
            with self.connection.cursor() as cursor:
                sql = """
                    INSERT INTO FlightTable (
                        TakeoffTime, TakeoffTimeBoot, LandingTime, Duration, PilotID,  # noqa
                        TakeoffLocation, LandingLocation, GSOID, version, FlightID     # noqa
                    )
                    VALUES (%s, %s, %s, %s, %s, ST_PointFromText(%s), ST_PointFromText(%s), %s, %s, %s)   # noqa
                """
                takeoff_point = f"POINT({flight_data['TakeoffLong']} {flight_data['TakeoffLat']})"   # noqa
                landing_point = f"POINT({flight_data['LandingLong']} {flight_data['LandingLat']})"   # noqa

                cursor.execute(sql, (
                    flight_data['TakeoffTimestampStr'],
                    flight_data['BootTimestampStr'],
                    flight_data['LandingTimestampStr'],
                    flight_data['TotalFlightTime'],
                    flight_data['PilotUID'],
                    takeoff_point, landing_point,
                    flight_data['GSOUID'],
                    flight_data['Version'],
                    flight_data['FlightID']
                ))

                # Get the last inserted UID
                uid = cursor.lastrowid
                self.connection.commit()
                logger.debug(f"Record added successfully with UID: {uid}")
                return uid
        except Exception as e:
            logger.error(f"{e}")
            return None
        finally:
            self.close_connection()

    def add_flight_file_record(self, flight_uid: str, log_uid: str) -> bool:
        """Add a record to the FlightFile table linking flight, log entries."""
        query = "INSERT INTO FlightFile (FlightID, LogID) VALUES (%s, %s)"
        return self.execute_insert_or_update(query, (flight_uid, log_uid))

    def get_aircraft_details_by_log_uid(self, log_uid: str
                                        ) -> Optional[Dict[str, str]]:
        """
        Retrieve aircraft details and Aircraft Model details based on LogUID.

        :param log_uid: The unique identifier of the log entry.
        :return: Dictionary containing aircraft and model details, or None.
        """
        # From LogTable using LogUID, get AircraftID
        query = "SELECT AircraftID FROM LogTable WHERE UID = %s"
        result = self.execute_query(query, (log_uid,), fetchone=True)
        if not result:
            logger.info(f"FAILED: No AircraftID found for LogUID: {log_uid}")
            return None
        aircraft_uid = result[0]
        logger.debug(f"AircraftUID: {aircraft_uid}")

        # From AircraftTable using AircraftID, get the data for the aircraft
        query = "SELECT * FROM AircraftTable WHERE UID = %s"
        result = self.execute_query(query, (aircraft_uid,), fetchone=True)
        if not result:
            logger.info(f"FAILED: No aircraft found for LogUID: {log_uid}")
            return None
        aircraft_details = result
        aircraft_model_uid = aircraft_details[3]
        logger.debug(f"AircraftModelUID: {aircraft_model_uid}")
        # From AircraftModel using AircraftModelID, get the data for the model
        query = "SELECT * FROM AircraftModel WHERE UID = %s"
        result = self.execute_query(query, (aircraft_model_uid,),
                                    fetchone=True)
        if not result:
            logger.info(f"FAILED: No model found for LogUID: {log_uid}")
            return None
        model_details = result

        # Combine the aircraft and model details into a single dictionary
        aircraft_details.update(model_details)
        return aircraft_details

    def get_all_logs_by_aircraft_uid(self, aircraft_id: int
                                     ) -> Optional[List[Dict[str, str]]]:
        """
        Retrieve all logs with a specific aircraft based on AircraftID.

        :param aircraft_id: The unique identifier of the aircraft.
        :return: List of dictionaries containing log details, or None.
        """
        try:
            query = """
                SELECT SHA256Hash, LogType, LogFileName, LogFileSize, StartTime, PilotComments, S3FileLocation # noqa
                FROM LogTable
                WHERE AircraftID = %s
            """
            result = self.execute_query(query, (aircraft_id,))

            if not result:
                logger.info(f"FAILED: No logs found AircraftID: {aircraft_id}")
                return None

            # Convert each result tuple to a dictionary
            logs = [dict(zip([col[0] for col in self.cursor.description],
                             row)) for row in result]

            return logs

        except Exception as e:
            logger.error(f"Error for logs for AircraftID {aircraft_id}: {e}")
            return None

    def get_all_flights_by_aircraft_uid(self, aircraft_id: int
                                        ) -> Optional[Dict[str, str]]:
        """
        Retrieve all flights associated with a aircraft based on AircraftID.

        :param aircraft_id: The unique identifier of the aircraft.
        :return: Dictionary containing flight details, or None if not found.
        """
        try:
            query = """
                SELECT PilotID, UTCTimeOffset, TakeoffTime, TakeoffTimeBoot, LandingTime,   # noqa
                    TakeoffLocation, LandingLocation, Duration, GSOID, version, FlightID    # noqa
                FROM FlightTable
                WHERE UID IN (
                    SELECT FlightID FROM FlightFile WHERE LogID IN (
                        SELECT UID FROM LogTable WHERE AircraftID = %s
                    )
                )
            """
            result = self.execute_query(query, (aircraft_id,))

            if not result:
                logger.info(f"FAILED: No flights found "
                            f"for AircraftID: {aircraft_id}")
                return None

            return result

        except Exception as e:
            logger.error(f"Error for flight AircraftID {aircraft_id}: {e}")
            return None

    def get_all_summary_for_flight_uid(self, flight_uid: int
                                       ) -> Optional[Dict[str, str]]:
        """
        Retrieve all summaries for a specific flight based on FlightUID.

        :param flight_uid: The unique identifier of the flight.
        :return: Dictionary containing summary details, or None if not found.
        """
        try:
            query = """
                SELECT LogID, FlightID, Message, Value, Unit, Version, TypeID, ProcessedDate, Instance # noqa
                FROM SummaryTable
                WHERE LogID IN (
                    SELECT LogID FROM FlightFile WHERE FlightID = %s
                )
            """
            result = self.execute_query(query, (flight_uid,))

            if not result:
                logger.info(f"FAILED: No summaries found for "
                            f"FlightUID: {flight_uid}")
                return None

            return result
        except Exception as e:
            logger.error(f"Error retrieving summary {flight_uid}: {e}")
            return None

    def get_all_errors_for_flight_uid(self, flight_uid: int
                                      ) -> Optional[Dict[str, str]]:
        """
        Retrieve all errors associated with a specific flight based FlightUID.

        :param flight_uid: The unique identifier of the flight.
        :return: Dictionary containing error details, or None if not found.
        """
        try:
            query = """
                SELECT LogID, FlightID, Message, Severity, ErrorStartTimestamp, ErrorClearTimestamp, # noqa
                ErrorDuration, Comment, ReportedByUserID, ResolvedStatus, ResolvedUserID,  # noqa
                ResolutionComment, Version, TypeID, ProcessedDate, Instance
                FROM ErrorTable
                WHERE LogID IN (
                    SELECT LogID FROM FlightFile WHERE FlightID = %s
                )
            """
            result = self.execute_query(query, (flight_uid,))

            if not result:
                logger.info(f"FAILED: No errors found for "
                            f"FlightUID: {flight_uid}")
                return None

            return result
        except Exception as e:
            logger.error(f"Error retrieving error FlightUID {flight_uid}: {e}")
            return None

    def get_aircraft_uid_from_cubeid(self, cube_id: str,
                                     timestamp: str) -> Optional[str]:
        """
        Retrieve the aircraft UID based on the CubeID and timestamp.

        :param cube_id: The unique identifier of the Cube.
        :param timestamp: The timestamp of the log.
        :return: The UID of the aircraft, or None if not found.
        """
        try:
            query = """
                SELECT 
                    ASCL.AircraftID
                FROM 
                    AircraftSubComponentLink AS ASCL
                JOIN 
                    SubComponentUnits AS SCU
                    ON ASCL.SubComponentUnitID = SCU.UID
                WHERE 
                    SCU.SerialNumber = %s
                    AND ASCL.StartDate <= FROM_UNIXTIME(%s)
                    AND (ASCL.EndDate IS NULL OR ASCL.EndDate >= FROM_UNIXTIME(%s)); # noqa
            """
            result = self.execute_query(query, (cube_id, timestamp, timestamp),
                                        fetchone=True)

            if not result:
                logger.info(f"FALIED: No aircraft found for CubeID: {cube_id}")
                return None

            return result[0]
        except Exception as e:
            logger.error(f"Error retrieving aircraft- CubeID {cube_id}: {e}")
            return None

    def get_aircraft_name_from_cubeid(self, cube_id: str,
                                      timestamp: str) -> Optional[str]:
        """
        Retrieve the aircraft name based on the CubeID and timestamp.

        :param cube_id: The unique identifier of the Cube.
        :param timestamp: The timestamp of the log.
        :return: The name of the aircraft, or None if not found.
        """
        try:
            query = """
                SELECT 
                    AT.AircraftName
                FROM 
                    AircraftSubComponentLink AS ASCL
                JOIN 
                    SubComponentUnits AS SCU
                    ON ASCL.SubComponentUnitID = SCU.UID
                JOIN
                    AircraftTable AS AT
                    ON ASCL.AircraftID = AT.UID
                WHERE 
                    SCU.SerialNumber = %s
                    AND ASCL.StartDate <= FROM_UNIXTIME(%s)
                    AND (ASCL.EndDate IS NULL OR ASCL.EndDate >= FROM_UNIXTIME(%s));  # noqa
            """
            result = self.execute_query(query, (cube_id, timestamp, timestamp),
                                        fetchone=True)

            if not result:
                logger.info(f"FALIED: No aircraft found for CubeID: {cube_id}")
                return None

            return result[0]
        except Exception as e:
            logger.error(f"Error retrieving aircraft- CubeID {cube_id}: {e}")
            return None

    def get_aircraft_row_by_cubeid(self, cube_id: str,
                                   timestamp: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve the full row from AircraftTable based on the CubeID,timestamp.

        :param cube_id: The unique identifier of the Cube.
        :param timestamp: The timestamp of the log.
        :return: A dictionary containing the full row AircraftTable, or None.
        """
        try:
            query = """
                SELECT 
                    AT.*
                FROM 
                    AircraftSubComponentLink AS ASCL
                JOIN 
                    SubComponentUnits AS SCU
                    ON ASCL.SubComponentUnitID = SCU.UID
                JOIN
                    AircraftTable AS AT
                    ON ASCL.AircraftID = AT.UID
                WHERE 
                    SCU.SerialNumber = %s
                    AND ASCL.StartDate <= FROM_UNIXTIME(%s)
                    AND (ASCL.EndDate IS NULL OR ASCL.EndDate >= FROM_UNIXTIME(%s));   # noqa
            """
            # Execute the query and fetch the result
            result = self.execute_query(query, (cube_id, timestamp, timestamp),
                                        fetchone=True)

            if not result:
                logger.info(f"FAILED: No aircraft found for CubeID: {cube_id}")
                return None

            # Get column names from the cursor description
            with self.connection.cursor() as cursor:
                cursor.execute(query, (cube_id, timestamp, timestamp))
                column_names = [desc[0] for desc in cursor.description]

            # Combine column names and result values into a dictionary
            row_dict = dict(zip(column_names, result))
            return row_dict

        except Exception as e:
            logger.error(f"Error retrieving aircraft row for CubeID"
                         f" {cube_id}: {e}")
            return None

    def __del__(self):
        """Destructor to ensure the connection is closed."""
        self.close_connection()


if __name__ == "__main__":
    logging.info("Starting AuroraHandler test")
    logger.setLevel(logging.DEBUG)

    DB_CREDENTIALS = {
        'host': 'temp',
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD', ""),
        'dbname': os.getenv('DB_NAME'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'region': os.getenv('AWS_REGION', 'ap-southeast-2'),
        'secret_name': os.getenv('DB_SECRET_NAME'),
    }
    if (not DB_CREDENTIALS['host'] or not DB_CREDENTIALS['username'] or
            not DB_CREDENTIALS['dbname'] or not DB_CREDENTIALS['secret_name']):
        logger.error("Missing required environment variables.")
        exit(1)

    db_handler = AuroraHandler(DB_CREDENTIALS)
    if not db_handler.init_state:
        logger.error("Failed to initialize AuroraHandler")

    # testing reconnection
    while True:
        if db_handler.connected():
            result = db_handler.get_aircraft_name_from_cubeid(
                '001F003A 34305107 35383431', '1732066788.992812')
            logger.debug(f"Result: {result}")
            break
        else:
            logger.error("Database connection failed.")
            DB_CREDENTIALS = {
                'host': os.getenv('DB_HOST'),
                'username': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD', ""),
                'dbname': os.getenv('DB_NAME'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'region': os.getenv('AWS_REGION', 'ap-southeast-2'),
                'secret_name': os.getenv('DB_SECRET_NAME'),
            }
            db_handler.db_credentials = DB_CREDENTIALS
            db_handler.reconnect()

    result = db_handler.get_aircraft_uid_from_cubeid(
        '001F003A 34305107 35383431', '1732066788.992812')
    logger.debug(f"Result: {result}")
    logger.debug(f"Result: {result}")
    result = db_handler.get_aircraft_row_by_cubeid(
        '001F003A 34305107 35383431', '1732066788.992812')
    logger.debug(f"Result: {result}")
    result = db_handler.get_aircraft_row_by_cubeid(
        '004A002B 34395106 35333839', '1638835117.593511')
    logger.debug(f"Result: {result}")
    result = db_handler.log_exists(
        '546d6435f057ec5c90880286390203a9833657c3c7ea6112bded2ac1d78e8b02')
    logger.debug(f"Result: {result}")
    result = db_handler.get_aircraft_row_by_cubeid(
        '001F003A 34305107 35383431', '1732066788.992812')
    logger.debug(f"Result: {result}")
