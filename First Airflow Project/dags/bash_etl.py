from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# Project data directory
BASE_DIR = "/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project/data/bash_etl"

# Create the directory if it does not exist
os.makedirs(BASE_DIR, exist_ok=True)


# DAG arguments
default_args = {
    "owner": "your_name",
    "start_date": datetime(2026, 9, 24),
    "email": ["your_email"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Define the DAG
with DAG(
    dag_id="my-bash-dag",
    default_args=default_args,
    description="My first BashOperator DAG",
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    # Extract task
    extract = BashOperator(
        task_id="extract",
        bash_command=f'cut -d":" -f1,3,6 /etc/passwd > "{BASE_DIR}/extracted-data.txt"',
    )

    # Transform and load task
    transform_and_load = BashOperator(
        task_id="transform",
        bash_command=f'tr ":" "," < "{BASE_DIR}/extracted-data.txt" > "{BASE_DIR}/transformed-data.csv"',
    )

    # Task pipeline
    extract >> transform_and_load