"""
Module to load records from local to Snowflake.
"""

import logging
from typing import List

import snowflake.connector
from airflow.decorators import task

from utils import config

logger = logging.getLogger(__name__)


def _get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
    )


@task
def load_to_snowflake(**kwargs):
    """
    Load records from local to Snowflake.
    Args:
        table (str): Table name.
    """

    file_paths: List[str, List[str]] = kwargs["ti"].xcom_pull(
        task_ids="extract_from_bucket"
    )

    with _get_snowflake_connection() as conn:
        cursor = conn.cursor()

        for table, files in file_paths.items():
            if not files:
                continue

            logger.info("Loading data to Snowflake for table: %s", table)

            for file in files:
                cursor.execute(f"PUT file://{file} @%{table}")

            copy_sql = f"""
                COPY INTO {table}
                FROM @%{table}
                FILE_FORMAT = (TYPE = 'PARQUET')
                ON_ERROR = 'CONTINUE'
            """

            cursor.execute(copy_sql)

    logger.info("✓ Data loaded successfully to Snowflake")
