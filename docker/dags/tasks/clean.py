import os
import shutil

from airflow.decorators import task

from .extract import LOCAL_OUTPUT_DIR


@task
def clean():
    """Clean up temporary files."""
    if os.path.exists(LOCAL_OUTPUT_DIR):
        shutil.rmtree(LOCAL_OUTPUT_DIR)
