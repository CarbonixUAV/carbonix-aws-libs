import logging
import os
from datetime import datetime
from typing import Dict, Optional

import boto3
from botocore.exceptions import NoCredentialsError

logger = logging.getLogger(__name__)


class S3Handler:
    def __init__(self, aws_region: str = "ap-southeast-2"):
        """Initialize the S3 handler with a given AWS region."""
        logger.debug("Initializing S3Handler...")
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.s3_resource = boto3.resource('s3', region_name=aws_region)
        logger.info("S3Handler initialized.")

    def get_s3_file_metadata(self, bucket_name: str, object_key: str) -> Optional[Dict[str, str]]:
        """
        Get the metadata of an S3 file.
        """
        try:
            response = self.s3_client.head_object(
                Bucket=bucket_name, Key=object_key)
            metadata = response.get('Metadata', {})
            return metadata
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(
                f"The object {object_key} does not exist in bucket {bucket_name}.")
            return None
        except Exception as e:
            logger.error(f"Error retrieving metadata: {e}")
            return None

    def copy_file_s3_to_s3(self, source_bucket: str, source_key: str,
                           destination_bucket: str, destination_key: str) -> bool:
        """
        Copy a file from one S3 bucket to another.
        """
        try:
            logger.debug(
                f"Copying {source_key} from {source_bucket} to {destination_bucket}/{destination_key}")
            self.s3_client.copy_object(
                CopySource={"Bucket": source_bucket, "Key": source_key},
                Bucket=destination_bucket,
                Key=destination_key
            )
            logger.info(
                f"Copied {source_key} to {destination_bucket}/{destination_key}")
            return True
        except NoCredentialsError:
            logger.error("S3 Credentials not available")
            return False
        except Exception as e:
            logger.error(f"Error copying file from S3 to S3: {e}")
            return False

    def download_file_s3(self, bucket_name: str, object_key: str, download_path: str) -> bool:
        """
        Download a file from S3 to a local path.
        """
        try:
            logger.debug(
                f"Downloading {object_key} from {bucket_name} to {download_path}")
            self.s3_client.download_file(
                bucket_name, object_key, download_path)
            logger.info(f"Downloaded {object_key} to {download_path}")
            return True
        except NoCredentialsError:
            logger.error("S3 Credentials not available")
            return False
        except Exception as e:
            logger.error(f"Error downloading file from S3: {e}")
            return False

    def upload_directory_s3(self, directory_path: str, bucket_name: str, s3_prefix: str = "") -> bool:
        """
        Upload the contents of a local directory to an S3 bucket.
        """
        logger.debug(
            f"Uploading contents of {directory_path} to {bucket_name}/{s3_prefix}")
        for root, _, files in os.walk(directory_path):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, directory_path)
                s3_path = os.path.join(
                    s3_prefix, relative_path).replace("\\", "/")
                try:
                    self.s3_client.upload_file(
                        local_path, bucket_name, s3_path)
                except NoCredentialsError:
                    logger.error("S3 Credentials not available")
                    return False
                except Exception as e:
                    logger.error(f"Error uploading file to S3: {e}")
                    return False
        logger.info(
            f"Uploaded contents of {directory_path} to {bucket_name}/{s3_prefix}")
        return True

    def upload_file_s3(self, file_path: str, bucket_name: str, object_key: str) -> bool:
        """
        Upload a file to an S3 bucket.
        """
        try:
            logger.debug(
                f"Uploading {file_path} to {bucket_name}/{object_key}")
            self.s3_client.upload_file(file_path, bucket_name, object_key)
            logger.info(f"Uploaded {file_path} to {bucket_name}/{object_key}")
            return True
        except NoCredentialsError:
            logger.error("S3 Credentials not available")
            return False
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False

    def delete_file_s3(self, bucket_name: str, object_key: str) -> bool:
        """
        Delete a file from an S3 bucket.
        """
        try:
            logger.debug(f"Deleting {object_key} from {bucket_name}")
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            logger.info(f"Deleted {object_key} from {bucket_name}")
            return True
        except NoCredentialsError:
            logger.error("S3 Credentials not available")
            return False
        except Exception as e:
            logger.error(f"Error deleting file from S3: {e}")
            return False

    def check_s3_item_exists(self, bucket_name: str, item_name: str) -> bool:
        """
        Check if a file or folder exists in an S3 bucket.
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            objs = list(bucket.objects.filter(Prefix=item_name))
            if len(objs) > 0:
                # If checking for a folder, ensure the prefix matches exactly
                if item_name.endswith('/'):
                    return any(obj.key.startswith(item_name) for obj in objs)
                else:
                    return any(obj.key == item_name for obj in objs)
            return False
        except Exception as e:
            logger.error(f"Error checking item existence in S3: {e}")
            return False

    def upload_unprocessed_s3(self, source_bucket: str, source_key: str,
                              unprocessed_destination_bucket: str,
                              s3_prefix: str = "NoCategory") -> bool:
        """
        Upload a file to an S3 bucket and move it to an unprocessed folder.
        """
        current_timestamp_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        destination_key = f"{s3_prefix}/{current_timestamp_str}/{source_key}"

        if self.copy_file_s3_to_s3(source_bucket, source_key, unprocessed_destination_bucket, destination_key):
            return self.delete_file_s3(source_bucket, source_key)

        return False
    
    def list_s3_folders(self, bucket_name: str) -> list:
        """
        List all folders in an S3 bucket.
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            folders = set()
            for obj in bucket.objects.all():
                if obj.key.endswith('/'):
                    folders.add(obj.key)
                else:
                    folder = '/'.join(obj.key.split('/')[:-1]) + '/'
                    if folder != '/':
                        folders.add(folder)
            return sorted(folders)
        except Exception as e:
            logger.error(f"Error listing folders in S3 bucket '{bucket_name}': {e}")
            return []

    def list_s3_files(self, bucket_name: str) -> list:
        """
        List all files in an S3 bucket.
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            files = [obj.key for obj in bucket.objects.all()]
            return files
        except Exception as e:
            logger.error(f"Error listing files in S3 bucket '{bucket_name}': {e}")
            return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    for lib in ('boto3', 'botocore', 'urllib3', 'pandas',
            'pyarrow', 'pymavlink', 's3transfer'):
        # Change from DEBUG to WARNING
        logging.getLogger(lib).setLevel(logging.WARNING)

    s3_handler = S3Handler()
    bucket_name = 'cx-dev-all-logs'
    item_name = 'V95_8_20240920-034906.bin'

    exists = s3_handler.check_s3_item_exists(bucket_name, item_name)
    if exists:
        logger.info(f"The item '{item_name}' exists in the bucket '{bucket_name}'.")
    else:
        logger.info(f"The item '{item_name}' does not exist in the bucket '{bucket_name}'.")
