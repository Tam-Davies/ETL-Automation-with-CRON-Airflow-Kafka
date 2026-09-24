# Import libraries
from datetime import timedelta
import pendulum

# Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAGS argument
default_args = {
    "owner": "Tam-Davies",
    "start_date": pendulum.today("UTC"),
    "email": ["your-email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule="@daily",
    catchup=False,
)

# Task 1: Unzip the downloaded data
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="""
    mkdir -p "$AIRFLOW_HOME/data/raw" &&
    tar -xzf "$AIRFLOW_HOME/tolldata.tgz" -C "$AIRFLOW_HOME/data/raw"
    """,
    dag=dag,
)

# Task 2: Extract required fields from CSV
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command="""
    cut -d',' -f1,2,3,4 \
    "$AIRFLOW_HOME/data/raw/vehicle-data.csv" \
    > "$AIRFLOW_HOME/data/extracted/csv_data.csv"
    """,
    dag=dag,
)

# Task 3: Extract required fields from TSV
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="""
    cut -f5,6,7 \
    "$AIRFLOW_HOME/data/raw/tollplaza-data.tsv" \
    > "$AIRFLOW_HOME/data/extracted/tsv_data.csv"
    """,
    dag=dag,
)


# Task 4: Extract required fields from fixed-width file
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="""
    cut -c59-60,66-67 \
    "$AIRFLOW_HOME/data/raw/payment-data.txt" \
    > "$AIRFLOW_HOME/data/extracted/fixed_width_data.csv"
    """,
    dag=dag,
)

# Task 5: Consolidate extracted data
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command="""
    paste -d',' \
    "$AIRFLOW_HOME/data/extracted/csv_data.csv" \
    "$AIRFLOW_HOME/data/extracted/tsv_data.csv" \
    "$AIRFLOW_HOME/data/extracted/fixed_width_data.csv" \
    > "$AIRFLOW_HOME/data/extracted/extracted_data.csv"
    """,
    dag=dag,
)

# Task 6: Transform vehicle_type to uppercase
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="""
    tr '[:lower:]' '[:upper:]' \
    < "$AIRFLOW_HOME/data/extracted/extracted_data.csv" \
    > "$AIRFLOW_HOME/data/staging/transformed_data.csv"
    """,
    dag=dag,
)

# Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data