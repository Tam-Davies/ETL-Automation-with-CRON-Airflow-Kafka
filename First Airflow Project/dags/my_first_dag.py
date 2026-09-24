from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


BASE_DIR = "/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project/data/first_etl"

input_file = "/etc/passwd"
extracted_file = os.path.join(BASE_DIR, "extracted-data.txt")
transformed_file = os.path.join(BASE_DIR, "transformed.txt")
output_file = os.path.join(BASE_DIR, "data_for_analytics.csv")

def extract():
    print("Inside Extract")

    with open(input_file, "r") as infile, \
            open(extracted_file, "w") as outfile:

        for line in infile:
            fields = line.split(":")

            if len(fields) >= 6:
                field_1 = fields[0]
                field_3 = fields[2]
                field_6 = fields[5]

                outfile.write(
                    field_1 + ":" + field_3 + ":" + field_6 + "\n"
                )


def transform():
    print("Inside Transform")

    with open(extracted_file, "r") as infile, \
            open(transformed_file, "w") as outfile:

        for line in infile:
            processed_line = line.replace(":", ",")
            outfile.write(processed_line)


def load():
    print("Inside Load")

    with open(transformed_file, "r") as infile, \
            open(output_file, "w") as outfile:

        for line in infile:
            outfile.write(line)


def check():
    print("Inside Check")

    with open(output_file, "r") as infile:
        for line in infile:
            print(line)


default_args = {
    "owner": "Your name",
    "start_date": datetime(2026, 9, 24),
    "email": ["your email"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="my-first-python-etl-dag",
    default_args=default_args,
    description="My first DAG",
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    execute_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    execute_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    execute_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    execute_check = PythonOperator(
        task_id="check",
        python_callable=check,
    )

    execute_extract >> execute_transform >> execute_load >> execute_check