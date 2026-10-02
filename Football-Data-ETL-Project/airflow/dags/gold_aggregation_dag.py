from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="gold_aggregation",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "batch"],
) as dag:

    run_gold_job = SparkSubmitOperator(
        task_id="run_gold_aggregate",
        application="/opt/airflow/src/football_etl/spark/gold_aggregate.py",
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.3",
        verbose=True,
    )
