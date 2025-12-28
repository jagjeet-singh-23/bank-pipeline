"""
MinIO client utilities for Airflow DAGs.
Provides minimal S3/MinIO operations needed by DAG tasks.
"""

from pathlib import Path
import boto3
from utils import config


def get_client() -> boto3.client:
    """
    Get configured boto3 S3 client for MinIO.

    Returns:
        boto3.client: Configured S3 client
    """
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )


def download_file(bucket: str, key: str, local_path: Path) -> None:
    """
    Download a file from MinIO to local path.

    Args:
        bucket (str): Bucket name
        key (str): Object key in bucket
        local_path (Path): Local file path to save to
    """
    client = get_client()
    client.download_file(bucket, key, str(local_path))
