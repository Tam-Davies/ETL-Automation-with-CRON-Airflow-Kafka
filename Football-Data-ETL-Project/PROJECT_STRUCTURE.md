# Football-Data-ETL-Project — Full Structure

```
Football-Data-ETL-Project/
├── .env
├── .gitignore
├── pyvenv.cfg
├── docker-compose.yml
├── requirements.txt
│
├── src/
│   └── football_etl/
│       ├── __init__.py
│       ├── config.py              # env vars: API, Kafka, Postgres settings
│       ├── ingestion.py           # extract_multiple_seasons() - API fetch logic
│       ├── producer.py            # Kafka producer ONLY: ingestion.py -> Kafka topic
│       │                          #   no DB writes, no transforms here
│       │
│       └── spark/
│           ├── bronze_ingest.py       # Kafka -> Delta bronze (streaming)
│           ├── silver_transform.py    # bronze -> Postgres silver (streaming)
│           │                          #   houses your transform_match_data()
│           │                          #   logic, ported from pandas -> PySpark
│           ├── gold_aggregate.py      # silver -> Postgres gold (batch, Airflow-triggered)
│           └── loading.py             # shared JDBC write helpers, imported by
│                                       #   silver_transform.py / gold_aggregate.py
│                                       #   (not run standalone anymore)
│
├── airflow/
│   └── dags/
│       ├── gold_aggregation_dag.py    # spark-submit gold_aggregate.py every 15 min
│       └── bronze_compaction_dag.py   # daily OPTIMIZE/VACUUM on Delta bronze table
│
├── configs/
│   └── postgres_schemas.sql       # silver (TimescaleDB hypertable) + gold schemas
│
└── data/                          # local Delta bronze storage (or point at MinIO/S3 in prod)
    ├── bronze/
    └── checkpoints/
        ├── bronze_ingest/
        └── silver_transform/
```

## What changed from your original layout

- **Removed:** `grafana_consumer.py`, `postgres_consumer.py` — these treated Grafana/Postgres as
  independent Kafka consumers. Only Spark consumes from Kafka; Grafana and Power BI both just
  query Postgres tables.
- **`transform.py` → `spark/silver_transform.py`:** your pandas feature-engineering
  (match_year, total_goals, goal_difference, outcome) is rewritten as PySpark so it can run as a
  streaming job instead of a one-shot script.
- **`loading.py`:** kept, but now imported as helper functions inside the Spark jobs
  (`foreachBatch` JDBC writes) rather than run as its own process.
- **New:** `spark/gold_aggregate.py`, `spark/bronze_ingest.py`, `airflow/`, `configs/`,
  `docker-compose.yml` — the pieces that didn't exist yet.

## Data flow

```
ingestion.py --(API)--> producer.py --(Kafka: matches.raw)-->
    spark/bronze_ingest.py --(Delta)--> bronze/
    spark/silver_transform.py --(uses loading.py)--> Postgres silver --> Grafana
    spark/gold_aggregate.py (Airflow, batch) --(uses loading.py)--> Postgres gold --> Power BI
```
