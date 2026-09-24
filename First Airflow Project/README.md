# First Airflow Project

A hands-on Apache Airflow project built with WSL2 Ubuntu, Docker Desktop, and VS Code. The project contains three working ETL pipelines that demonstrate `PythonOperator`, `BashOperator`, task dependencies, scheduling, logging, and explicit data paths.

## Project Status

- Apache Airflow: `3.3.2`
- Python: `3.14.4`
- PostgreSQL: `18.6`
- Environment: Windows, WSL2 Ubuntu, Docker Desktop, VS Code
- DAGs: 3
- Pipeline status: all three DAGs have executed successfully

## Architecture

```text
Windows
  |
VS Code
  |
WSL2 Ubuntu
  |
Docker Desktop
  |
Apache Airflow
```

VS Code should be connected to the `WSL: Ubuntu` environment rather than the Docker Desktop WSL distribution.

## Project Structure

```text
First Airflow Project/
├── README.md
├── airflow.cfg
├── dags/
│   ├── bash_etl.py
│   ├── ETL_Server_Access_Log_Processing.py
│   └── my_first_dag.py
└── data/
    ├── bash_etl/
    │   ├── extracted-data.txt
    │   └── transformed-data.csv
    ├── first_etl/
    │   ├── extracted-data.txt
    │   ├── transformed.txt
    │   └── data_for_analytics.csv
    └── second_etl/
        ├── web-server-access-log.txt
        ├── extracted-data.txt
        ├── transformed.txt
        └── capitalized.txt
```

Airflow runtime files such as `airflow.db` and `logs/` are also generated in the project directory.

## Airflow Configuration

The project uses the following WSL path as `AIRFLOW_HOME`:

```text
/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project
```

The configured DAG folder is:

```text
/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project/dags
```

Set the project as `AIRFLOW_HOME` in a WSL terminal:

```bash
export AIRFLOW_HOME="/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project"
```

To make this setting permanent, add the export command to your shell profile, such as `~/.bashrc`.

## Starting Airflow

Activate the virtual environment:

```bash
source ~/airflow/airflow_venv/bin/activate
```

Confirm the project is being used as `AIRFLOW_HOME`:

```bash
echo "$AIRFLOW_HOME"
airflow config get-value core dags_folder
```

Start Airflow in standalone mode:

```bash
airflow standalone
```

Open the Airflow Web UI at:

```text
http://localhost:8080
```

The `airflow standalone` process must remain running while using the Web UI. After restarting the computer, activate the virtual environment, set `AIRFLOW_HOME`, start Airflow, and then open the Web UI again.

## DAGs and Pipelines

### 1. Python ETL

File: [`dags/my_first_dag.py`](dags/my_first_dag.py)

DAG ID: `my-first-python-etl-dag`

```text
extract -> transform -> load -> check
```

This pipeline reads `/etc/passwd`, extracts selected fields, transforms colon-separated values into comma-separated values, writes the analytics output, and prints the final data for verification. It uses `PythonOperator`.

Outputs:

- `data/first_etl/extracted-data.txt`
- `data/first_etl/transformed.txt`
- `data/first_etl/data_for_analytics.csv`

### 2. Web Server Access Log ETL

File: [`dags/ETL_Server_Access_Log_Processing.py`](dags/ETL_Server_Access_Log_Processing.py)

DAG ID: `my-second-python-etl-dag`

```text
download -> extract -> transform -> load -> check
```

This pipeline downloads `web-server-access-log.txt`, extracts selected fields, converts the extracted data to uppercase, writes the result, and prints the final output. It uses `PythonOperator` and the `requests` package.

Outputs:

- `data/second_etl/web-server-access-log.txt`
- `data/second_etl/extracted-data.txt`
- `data/second_etl/transformed.txt`
- `data/second_etl/capitalized.txt`

### 3. Bash ETL

File: [`dags/bash_etl.py`](dags/bash_etl.py)

DAG ID: `my-bash-dag`

```text
extract -> transform
```

This pipeline uses `BashOperator` to extract fields 1, 3, and 6 from `/etc/passwd` with `cut`, then converts the colon separators to commas with `tr`.

Commands used by the pipeline:

```bash
cut -d":" -f1,3,6 /etc/passwd
tr ":" ","
```

Outputs:

- `data/bash_etl/extracted-data.txt`
- `data/bash_etl/transformed-data.csv`

## Running a DAG

1. Start Airflow with `airflow standalone`.
2. Open `http://localhost:8080`.
3. Find the DAG by its DAG ID.
4. Enable the DAG if it is paused.
5. Trigger it manually from the Airflow Web UI.
6. Monitor task states and open task logs.
7. Confirm the generated files in the appropriate `data/` directory.

The DAGs are configured with a daily schedule and `catchup=False`.

## Airflow 3 Compatibility Notes

The original course examples used older Airflow syntax. This project uses syntax compatible with Airflow `3.3.2`:

- `datetime` is used instead of the removed `days_ago` helper.
- `schedule` is used instead of `schedule_interval`.
- `PythonOperator` is imported from `airflow.providers.standard.operators.python`.
- `BashOperator` is imported from `airflow.providers.standard.operators.bash`.

## Troubleshooting Lessons

### Unexpected output locations

Relative paths can cause Airflow to write files to its execution working directory instead of the project directory. The DAGs use explicit paths based on the project data directories, for example:

```python
BASE_DIR = "/mnt/c/Users/user/Desktop/Data Engineering/First Airflow Project/data/first_etl"
```

This keeps generated files predictable and easy to verify.

### WSL connection

When using VS Code, connect to `WSL: Ubuntu`. Connecting through `wsl+docker-desktop` can place the VS Code Server in the wrong environment and make project files or commands difficult to access.

### Finding misplaced files

If an output file cannot be found, search the WSL filesystem with:

```bash
find /home -type f \( -name "web-server-access-log.txt" -o -name "extracted-data.txt" -o -name "transformed.txt" -o -name "capitalized.txt" \) 2>/dev/null
```

## What I Learned

- A DAG defines a workflow made up of ordered tasks.
- Operators determine what each task does.
- `PythonOperator` runs Python functions.
- `BashOperator` runs Linux shell commands.
- Dependencies such as `extract >> transform >> load >> check` control task order.
- Airflow can run DAGs manually or on a schedule.
- Task logs are useful for diagnosing failures.
- Airflow orchestrates data work; Python, Bash, APIs, databases, and storage perform the underlying operations.
- A successful pipeline should be verified in both the Airflow Web UI and the generated data files.

## Workflow

```text
Write or edit a DAG in VS Code
        |
Save the DAG
        |
Airflow detects the DAG
        |
Trigger or schedule the DAG
        |
Airflow executes the tasks
        |
Monitor task status and logs
        |
Verify the output files
```

## Current Status

The Airflow environment is installed and working. VS Code is connected to WSL Ubuntu, the project is configured as `AIRFLOW_HOME`, and all three ETL pipelines have run successfully. The project provides a foundation for building more advanced data-engineering workflows.
