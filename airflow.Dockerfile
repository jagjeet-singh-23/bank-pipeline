# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt packages and DAG dependencies
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    boto3 \
    redis \
    snowflake-connector-python \
    pandas \
    fastparquet
