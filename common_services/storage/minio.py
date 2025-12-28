"""
MinIO storage class for uploading records to MinIO.
"""

from pathlib import Path
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List

import boto3
import pandas as pd

from constants import Constants
from .base import Storage

logger = logging.getLogger(__name__)


class MinIOStorage(Storage):
    """
    MinIO storage class for uploading records to MinIO.
    """

    def __init__(self) -> None:
        self.client = self._get_s3_client()
        self.bucket: str = str(Constants.MINIO_BUCKET)
        self._ensure_bucket()

    def _get_s3_client(self) -> boto3.client:
        return boto3.client(
            "s3",
            endpoint_url=Constants.MINIO_ENDPOINT,
            aws_access_key_id=Constants.MINIO_ACCESS_KEY,
            aws_secret_access_key=Constants.MINIO_SECRET_KEY,
        )

    def _ensure_bucket(self) -> None:
        buckets = [b["Name"] for b in self.client.list_buckets()["Buckets"]]
        if self.bucket not in buckets:
            self.client.create_bucket(Bucket=self.bucket)

    def upload_records(self, table: str, records: List[Dict[str, Any]]) -> None:
        """Method to upload records to MinIO.
        Args:
            table (str): Table name.
            records (List[Dict[str, Any]]): List of records to upload.
        Returns:
            None
        """
        df = pd.DataFrame(records)
        date_str: str = datetime.now().strftime("%Y-%m-%d")

        # Use uuid to ensure unique filename for every upload task
        local_file: str = f"{table}_{date_str}_{uuid.uuid4().hex}.parquet"
        df.to_parquet(local_file, engine="fastparquet", index=False)

        s3_key: str = f"{table}/date={date_str}/{table}_{datetime.now().strftime('%H%M%S%f')}.parquet"
        self.client.upload_file(local_file, self.bucket, s3_key)

        os.remove(local_file)
        logger.info("✅ Uploaded %s → %s", len(records), s3_key)

    def download_object(self, bucket: str, key: str, local_path: Path) -> None:
        """Method to download objects from MinIO.
        Args:
            bucket (str): Bucket name.
            key (str): Object key.
            local_path (Path): Local path to save the object."""
        self.client.download_file(bucket, key, local_path)


# Singleton instance of MinIOStorage
minio_storage = MinIOStorage()
