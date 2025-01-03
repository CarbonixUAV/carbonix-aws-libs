import logging
import pymysql
import json
import boto3
from typing import Optional, Tuple, Dict

logger = logging.getLogger(__name__)

class AuroraHandler:
    def __init__(self, db_credentials: Dict[str, str]):
        """
        Initialize the AuroraHandler instance with database credentials and establish a connection.
        """
        self.db_credentials = db_credentials
        self.connection = None
        self.init_state = False
        self.retrieve_db_credentials()
        self.init_state = self.connect()

    def retrieve_db_credentials(self) -> None:
        """Retrieve and store database credentials from AWS Secrets Manager."""
        client = boto3.session.Session().client(
            service_name='secretsmanager', region_name=self.db_credentials['region']
        )
        try:
            response = client.get_secret_value(SecretId=self.db_credentials['secret_name'])
            secret = json.loads(response['SecretString'])
            self.db_credentials['password'] = secret['password']
            logger.info("Secret retrieved successfully from Secrets Manager.")
        except Exception as e:
            logger.error(f"Error retrieving secret: {e}")

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
                logger.info("Database connection closed.")
            except pymysql.MySQLError as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.connection = None

    def execute_query(self, query: str, params: Optional[Tuple] = None, fetchone: bool = False):
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
            logger.info("Transaction committed.")
            return True
        except Exception as e:
            logger.error(f"Transaction error: {e}")
            self.connection.rollback()
            return False

    def log_exists(self, sha256hash: str) -> bool:
        """Check if a log exists in LogTable by SHA256Hash."""
        query = "SELECT COUNT(1) FROM LogTable WHERE SHA256Hash = %s"
        result = self.execute_query(query, (sha256hash,), fetchone=True)
        return result[0] > 0 if result else False

    def __del__(self):
        """Destructor to ensure the connection is closed."""
        self.close_connection()
