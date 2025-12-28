import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from airflow.decorators import task

from utils import minio_client, config

from .redis_state import (
    clear_continuation_token,
    get_continuation_token,
    set_continuation_token,
)

LOCAL_OUTPUT_DIR: Path = Path("/tmp/minio_downloads")
TABLES: List[str] = [
    "customers",
    "accounts",
    "transactions",
]


@task
def extract_from_bucket() -> Dict[str, List[str]]:
    """
    Extract files from MinIO bucket in batches
    """
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

    local_files: Dict[str, List[str]] = defaultdict(list)
    client = minio_client.get_client()

    for table in TABLES:
        token = get_continuation_token(table)

        list_params = {
            "Bucket": config.MINIO_BUCKET,
            "Prefix": f"{table}/",
            "MaxKeys": config.MAX_BATCH_SIZE,
        }

        if token:
            list_params["ContinuationToken"] = token

        resp = client.list_objects_v2(**list_params)

        objects = resp.get("Contents", [])
        for obj in objects:
            key = obj["Key"]
            local_file = LOCAL_OUTPUT_DIR / key

            local_file.parent.mkdir(parents=True, exist_ok=True)

            minio_client.download_file(config.MINIO_BUCKET, key, local_file)
            local_files[table].append(str(local_file))

        next_token = resp.get("NextContinuationToken")
        if next_token:
            set_continuation_token(table, next_token)
        else:
            clear_continuation_token(table)

    return local_files
