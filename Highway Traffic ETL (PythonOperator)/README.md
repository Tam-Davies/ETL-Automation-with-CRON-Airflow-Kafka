# Highway Traffic ETL (PythonOperator)

## Overview

This project implements an Apache Airflow ETL pipeline that processes highway toll data using Python-based tasks. The DAG downloads a compressed dataset, extracts the archive, parses files in different formats, consolidates the relevant fields, and transforms the data before storing it in the staging area.

The workflow is built with `PythonOperator`, which executes custom Python functions for each stage of the pipeline.

## Objective

The pipeline is designed to:

- download the source toll dataset
- extract raw files from a `.tgz` archive
- read CSV, TSV, and fixed-width data
- select the required data fields from each source
- consolidate the extracted records into a single output file
- transform the final dataset into uppercase format
- save the results in the Airflow working directories

## Pipeline Flow

The DAG named `ETL_toll_data` runs in the following sequence:

1. `download_dataset` downloads the archive from the source URL.
2. `untar_dataset` extracts the archive into the `data/raw/` directory.
3. `extract_data_from_csv` extracts selected columns from `vehicle-data.csv`.
4. `extract_data_from_tsv` extracts selected columns from `tollplaza-data.tsv`.
5. `extract_data_from_fixed_width` extracts values from the fixed-width text file.
6. `consolidate_data` merges the three extracted outputs into a single file.
7. `transform_data` converts the final dataset to uppercase.

## Data Sources

The source archive is downloaded as:

- `tolldata.tgz`

It contains:

- `vehicle-data.csv`
- `tollplaza-data.tsv`
- `payment-data.txt`

These files use different structures and therefore require separate extraction logic.

## Output Files

The pipeline writes files to the Airflow home directory structure:

- `data/raw/` - extracted source files
- `data/extracted/csv_data.csv` - CSV extraction output
- `data/extracted/tsv_data.csv` - TSV extraction output
- `data/extracted/fixed_width_data.csv` - fixed-width extraction output
- `data/extracted/extracted_data.csv` - consolidated dataset
- `data/staging/transformed_data.csv` - final transformed dataset

## Project Structure

```text
Highway Traffic ETL (PythonOperator)/
├── airflow.cfg
├── airflow.db
├── dags/
│   └── ETL_toll_data.py
├── data/
│   ├── raw/
│   ├── extracted/
│   └── staging/
├── logs/
├── simple_auth_manager_passwords.json.generated
├── tolldata.tgz
└── README.md
```

## DAG Details

The DAG is defined in:

- `dags/ETL_toll_data.py`

Key configuration:

- DAG ID: `ETL_toll_data`
- Schedule: `@daily`
- Catchup: `False`
- Retries: `1`
- Retry delay: `5 minutes`

The task dependencies are:

```python
download_task >> untar_task

untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task]

[extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
```

## How to Run

1. Start your Apache Airflow environment.
2. Ensure the `AIRFLOW_HOME` environment variable points to this project folder.
3. Confirm the DAG appears in the Airflow UI.
4. Trigger `ETL_toll_data` manually or wait for the scheduled run.
5. Review the generated files under `data/extracted/` and `data/staging/`.

## Technologies Used

- Python
- Apache Airflow
- CSV and TSV processing
- Fixed-width text parsing
- urllib and tarfile libraries
- Linux / WSL environment

## Notes

This project is a practical example of using Airflow with Python-based ETL tasks to handle multi-format source files and automate a recurring data processing job.
