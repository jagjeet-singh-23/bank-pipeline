"""
Module to ingest raw data from MinIO bucket to Snowflake bronze table.
The Airflow DAG is scheduled to run every 5 minutes.
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from tasks import extract_from_bucket, load_to_snowflake, clean

with DAG(
    dag_id="snowflake_ingestion_DAG",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
) as dag:
    EXTRACTION_TASK = extract_from_bucket()
    LOADING_TASK = load_to_snowflake(table="raw")
    CLEANING_TASK = clean()

    EXTRACTION_TASK >> LOADING_TASK >> CLEANING_TASK
