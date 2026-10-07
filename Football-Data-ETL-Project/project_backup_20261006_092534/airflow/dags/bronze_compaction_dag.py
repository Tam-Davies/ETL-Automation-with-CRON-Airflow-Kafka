from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=10)}

COMPACTION_SQL = """
OPTIMIZE delta.`/opt/data/bronze/matches`;
VACUUM delta.`/opt/data/bronze/matches` RETAIN 168 HOURS;
"""

with DAG(
    dag_id="bronze_compaction",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "maintenance"],
) as dag:

    compact_bronze = SparkSubmitOperator(
        task_id="optimize_and_vacuum_bronze",
        application="/opt/src/football_etl/spark/run_sql.py",
        application_args=[COMPACTION_SQL],
        conn_id="spark_default",
        verbose=True,
    )
