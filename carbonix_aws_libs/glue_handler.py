import logging
from typing import Optional, Dict
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class GlueCrawlerHandler:
    def __init__(self, crawler_name: str):
        self.glue_client = boto3.client('glue')
        self.crawler_name = crawler_name

    def start_crawler(self) -> Optional[Dict[str, str]]:
        """
        Trigger the AWS Glue crawler.

        :return: Response from the start_crawler API call or None if an error occurs.
        """
        try:
            response = self.glue_client.start_crawler(Name=self.crawler_name)
            logger.debug(f"Triggered the crawler: {self.crawler_name}")
            return response
        except ClientError as e:
            logger.debug(f"Error triggering the crawler: {e}")
            return None

    def get_crawler_status(self, crawler_name: str) -> Optional[str]:
        """
        Get the status of the AWS Glue crawler.

        :return: Status of the crawler or None if an error occurs.
        """
        try:
            response = self.glue_client.get_crawler(Name=crawler_name)
            status = response['Crawler']['State']
            logger.debug(f"Crawler {crawler_name} status: {status}")
            return status
        except ClientError as e:
            logger.debug(f"Error getting the crawler status: {e}")
            return None

    def is_crawler_running(self) -> bool:
        """
        Check if the AWS Glue crawler is currently running.

        :return: True if the crawler is running, False otherwise.
        """
        status = self.get_crawler_status(self.crawler_name)
        return status == 'RUNNING'

    def is_crawler_completed(self) -> bool:
        """
        Check if the AWS Glue crawler has completed its run.

        :return: True if the crawler has completed, False otherwise.
        """
        status = self.get_crawler_status(self.crawler_name)
        return status == 'READY'
