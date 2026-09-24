from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


import os

BASE_DIR = "/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project/data/second_etl"

input_file = os.path.join(BASE_DIR, "web-server-access-log.txt")
extracted_file = os.path.join(BASE_DIR, "extracted-data.txt")
transformed_file = os.path.join(BASE_DIR, "transformed.txt")
output_file = os.path.join(BASE_DIR, "capitalized.txt")


def download_file():
    url = (
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
        "IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/"
        "Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    )

    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        with open(input_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    print(f"File downloaded successfully: {input_file}")


def extract():
    print("Inside Extract")

    with open(input_file, "r") as infile, \
            open(extracted_file, "w") as outfile:

        for line in infile:
            fields = line.split("#")

            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]

                outfile.write(
                    field_1 + "#" + field_4 + "\n"
                )


def transform():
    print("Inside Transform")

    with open(extracted_file, "r") as infile, \
            open(transformed_file, "w") as outfile:

        for line in infile:
            processed_line = line.upper()
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
    dag_id="my-second-python-etl-dag",
    default_args=default_args,
    description="Download, extract, transform, load and check web server log data",
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_file,
    )

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

    download >> execute_extract >> execute_transform >> execute_load >> execute_check