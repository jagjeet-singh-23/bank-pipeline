import sys
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

load_dotenv(dotenv_path=PROJECT_ROOT / ".env")


class Constants:
    # POSTGRES constants
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # DEBEZIUM constants
    DEBEZIUM_PORT = os.getenv("DEBEZIUM_PORT")
    DEBEZIUM_HOST = os.getenv("DEBEZIUM_HOST")

    # Kafka
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
    KAFKA_GROUP = os.getenv("KAFKA_GROUP")

    # MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET")
    MINIO_LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR")

    # DLQ
    DLQ_TOPIC: str = "banking_server.dlq"

    # Batch
    BATCH_SIZE: int = 50
    MAX_IN_FLIGHT_UPLOADS: int = 5
    UPLOAD_QUEUE_SIZE: int = 10

    # Snowflake Credentials
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))

    # Extraction Batch Processing
    MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "50"))


__all__ = ["Constants"]
