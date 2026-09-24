from datetime import timedelta
import os
import tarfile
import csv
import urllib.request
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


# ==================================================
# Python Functions
# ==================================================

def download_dataset():
    """Download the toll data archive."""

    url = (
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
        "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    )

    output_file = os.path.join(
        os.environ["AIRFLOW_HOME"],
        "tolldata.tgz"
    )

    urllib.request.urlretrieve(url, output_file)


def untar_dataset():
    """Extract the downloaded tar archive."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    archive = os.path.join(
        airflow_home,
        "tolldata.tgz"
    )

    raw_dir = os.path.join(
        airflow_home,
        "data",
        "raw"
    )

    os.makedirs(raw_dir, exist_ok=True)

    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(raw_dir)


def extract_data_from_csv():
    """Extract selected fields from vehicle-data.csv."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    input_file = os.path.join(
        airflow_home,
        "data",
        "raw",
        "vehicle-data.csv"
    )

    output_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "csv_data.csv"
    )

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, "r") as infile:
        reader = csv.reader(infile)

        with open(output_file, "w", newline="") as outfile:
            writer = csv.writer(outfile)

            for row in reader:
                writer.writerow(row[:4])


def extract_data_from_tsv():
    """Extract fields 5, 6 and 7 from tollplaza-data.tsv."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    input_file = os.path.join(
        airflow_home,
        "data",
        "raw",
        "tollplaza-data.tsv"
    )

    output_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "tsv_data.csv"
    )

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, "r") as infile:
        reader = csv.reader(infile, delimiter="\t")

        with open(output_file, "w", newline="") as outfile:
            writer = csv.writer(outfile)

            for row in reader:
                writer.writerow(row[4:7])


def extract_data_from_fixed_width():
    """Extract vehicle number and payment code from fixed-width data."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    input_file = os.path.join(
        airflow_home,
        "data",
        "raw",
        "payment-data.txt"
    )

    output_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "fixed_width_data.csv"
    )

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, "r") as infile:
        with open(output_file, "w") as outfile:

            for line in infile:
                vehicle_number = line[58:60]
                payment_code = line[65:67]

                outfile.write(
                    f"{vehicle_number},{payment_code}\n"
                )


def consolidate_data():
    """Combine the three extracted datasets."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    csv_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "csv_data.csv"
    )

    tsv_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "tsv_data.csv"
    )

    fixed_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "fixed_width_data.csv"
    )

    output_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "extracted_data.csv"
    )

    with open(csv_file, "r") as csv_in, \
         open(tsv_file, "r") as tsv_in, \
         open(fixed_file, "r") as fixed_in, \
         open(output_file, "w") as outfile:

        for csv_line, tsv_line, fixed_line in zip(
            csv_in,
            tsv_in,
            fixed_in
        ):
            outfile.write(
                csv_line.strip()
                + ","
                + tsv_line.strip()
                + ","
                + fixed_line.strip()
                + "\n"
            )


def transform_data():
    """Transform vehicle type values to uppercase."""

    airflow_home = os.environ["AIRFLOW_HOME"]

    input_file = os.path.join(
        airflow_home,
        "data",
        "extracted",
        "extracted_data.csv"
    )

    output_file = os.path.join(
        airflow_home,
        "data",
        "staging",
        "transformed_data.csv"
    )

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, "r") as infile:
        with open(output_file, "w") as outfile:

            for line in infile:
                outfile.write(line.upper())


# ==================================================
# DAG Definition
# ==================================================

default_args = {
    "owner": "Tam-Davies",
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Highway Traffic ETL using PythonOperator",
    schedule="@daily",
    catchup=False,
)


# ==================================================
# Tasks
# ==================================================

download_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag,
)

untar_task = PythonOperator(
    task_id="untar_dataset",
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id="extract_data_from_csv",
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id="extract_data_from_tsv",
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_width_task = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)


# ==================================================
# Task Dependencies
# ==================================================

download_task >> untar_task

untar_task >> [
    extract_csv_task,
    extract_tsv_task,
    extract_fixed_width_task
]

[
    extract_csv_task,
    extract_tsv_task,
    extract_fixed_width_task
] >> consolidate_task >> transform_task
