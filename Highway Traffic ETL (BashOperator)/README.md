# Highway Traffic ETL Pipeline with Apache Airflow

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow.

The project was developed as part of the IBM Data Engineering Final Assignment. The objective is to consolidate highway toll-plaza traffic data provided in different file formats into a single standardized dataset.

The pipeline extracts data from CSV, TSV, and fixed-width files, consolidates the extracted fields, transforms the vehicle type information, and stores the resulting dataset in a staging area.

## Project Objective

The objective of this project is to build an automated ETL workflow that:

- Extracts data from multiple file formats.
- Selects the required fields from each source.
- Consolidates the extracted data into a single dataset.
- Transforms the vehicle type field into uppercase.
- Stores the transformed dataset in a staging directory.
- Automates the workflow using Apache Airflow.

## Technologies Used

- Python
- Apache Airflow 3.3.2
- Bash
- Linux / WSL
- CSV
- TSV
- Fixed-width text files
- Git and GitHub

## Source Data

The source data is provided as a compressed archive:

`tolldata.tgz`

The archive contains the following data files:

- `vehicle-data.csv`
- `tollplaza-data.tsv`
- `payment-data.txt`

Each file uses a different data format.

### CSV Data

The `vehicle-data.csv` file contains vehicle-related information.

The following fields are extracted:

- Rowid
- Timestamp
- Anonymized Vehicle number
- Vehicle type

The extracted data is saved as:

`data/extracted/csv_data.csv`

### TSV Data

The `tollplaza-data.tsv` file contains toll-plaza information.

The following fields are extracted:

- Number of axles
- Tollplaza id
- Tollplaza code

The extracted data is saved as:

`data/extracted/tsv_data.csv`

### Fixed-width Data

The `payment-data.txt` file contains payment information in fixed-width format.

The following fields are extracted:

- Type of Payment code
- Vehicle Code

The extracted data is saved as:

`data/extracted/fixed_width_data.csv`

## Pipeline Workflow

The Airflow DAG runs the following tasks in sequence:

1. `unzip_data` extracts the source archive into `data/raw/`.
2. `extract_data_from_csv` selects the required fields from the CSV file.
3. `extract_data_from_tsv` selects the required fields from the TSV file.
4. `extract_data_from_fixed_width` selects character ranges from the fixed-width file.
5. `consolidate_data` combines the extracted files into one CSV file.
6. `transform_data` converts text, including the vehicle type field, to uppercase.

The consolidated dataset is saved as:

`data/extracted/extracted_data.csv`

The transformed dataset is saved as:

`data/staging/transformed_data.csv`

## Project Structure

```text
Highway Traffic ETL/
│
├── dags/
│   └── ETL_toll_data.py
│
├── data/
│   ├── raw/
│   ├── extracted/
│   ├── transformed/
│   └── staging/
│
├── logs/
│
├── README.md
│
└── .gitignore
```

## Running the Pipeline

1. Start the Airflow environment.
2. Place `tolldata.tgz` in the Airflow home directory.
3. Confirm that the DAG `ETL_toll_data` appears in the Airflow interface.
4. Trigger the DAG manually or wait for its daily schedule.
5. Review the generated files in `data/extracted/` and `data/staging/`.

The DAG is configured with a daily schedule and `catchup=False`.
