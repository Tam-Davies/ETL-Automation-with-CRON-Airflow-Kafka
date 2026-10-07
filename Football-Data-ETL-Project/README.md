# Football Data ETL

A containerized data engineering project that ingests English Premier League
match data from [football-data.org](https://www.football-data.org/), streams it
through Kafka and Spark, stores raw and transformed data, and builds team
summaries for reporting.

## Architecture

```text
football-data.org API
        |
        v
Python producer ---> Kafka topic
                         |
                         v
                 Spark Bronze stream ---> Delta files (data/bronze)
                         |
                         v
                 Spark Silver stream ---> TimescaleDB (silver.match_events)
                                                |
                                  Airflow every 15 minutes
                                                v
                                      gold.match_summary
```

- **Bronze:** raw Kafka records stored as Delta data.
- **Silver:** parsed match records with date attributes, scores, goals,
  goal difference, and match outcome.
- **Gold:** team-level played, win, loss, draw, points, and average statistics.
- **Grafana** and **Power BI** are intended to query Postgres; they are not
  Kafka consumers.

The long-running Bronze and Silver Spark streams are **not** started by
`docker compose up`. Start them separately as shown below. Airflow schedules
the Gold aggregation every 15 minutes and Bronze compaction daily.

## Requirements

- Docker Engine and the Docker Compose plugin
- A football-data.org API token
- Internet access the first time the Docker images and Spark packages are
  downloaded

## Configuration

Create a `.env` file in the project root. It is ignored by Git; do not commit
API tokens or passwords.

```dotenv
API_KEY_ACCESS=your_football_data_org_token
DB_PASSWORD=replace_with_a_strong_postgres_password
KAFKA_TOPIC_MATCHES=football-matches-topic

# Spark containers use these paths inside the mounted /opt/data directory.
BRONZE_PATH=/opt/data/bronze/matches
BRONZE_CHECKPOINT=/opt/data/checkpoints/bronze_ingest
SILVER_CHECKPOINT=/opt/data/checkpoints/silver_transform

# These defaults match the Docker Compose network and database.
PG_URL=jdbc:postgresql://postgres:5432/football_db
PG_USER=postgres
PG_PASSWORD=replace_with_a_strong_postgres_password
```

`DB_PASSWORD`, `API_KEY_ACCESS`, and `KAFKA_TOPIC_MATCHES` are required for the
provided Compose setup. `PG_URL`, `PG_USER`, and `PG_PASSWORD` have Compose or
application defaults, but set them explicitly as above so Spark can connect to
the Compose Postgres service. Keep `PG_PASSWORD` consistent with
`DB_PASSWORD`.

## Start the infrastructure

From the project root:

```bash
docker compose up -d --build
docker compose ps
```

The producer starts after Kafka is healthy, fetches the configured seasons, and
publishes the returned matches. It exits after publishing; this is expected.
Postgres initializes the schemas when its data volume is first created.

## Start the Spark streaming jobs

After the Compose services are up, run each command in a separate terminal.
These commands stay attached while the streaming queries run; use `Ctrl+C` to
stop an individual job.

Bronze: Kafka to Delta:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/src/football_etl/spark/bronze_ingest.py
```

Silver: Delta Bronze to Postgres Silver:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  /opt/src/football_etl/spark/silver_transform.py
```

The Spark jobs use checkpoints under `data/checkpoints/` to track streaming
progress. Keep these directories between restarts; deleting them can cause
previously processed Kafka or Delta records to be processed again.

## Services and local addresses

| Service | Address / port | Purpose |
| --- | --- | --- |
| Airflow UI | <http://localhost:8081> | View and monitor scheduled DAGs |
| Grafana | <http://localhost:3000> | Dashboards; configure a Postgres data source |
| Spark master UI | <http://localhost:8080> | View Spark applications and workers |
| Kafka | `localhost:9092` | Host-accessible Kafka broker |
| Postgres / TimescaleDB | `localhost:5433` | Host-accessible database |

Inside the Compose network, Postgres is available at `postgres:5432`, and Kafka
at `kafka:29092`. The database is `football_db`.

## Database schemas

- `silver.match_events` is a TimescaleDB hypertable keyed by match date.
- `gold.match_summary` contains the latest team-level aggregates.
- Schema creation scripts are in [`configs/`](configs/).

The SQL initialization script creates `grafana_reader` and `powerbi_reader`
with placeholder passwords. Change those passwords in
[`configs/postgres_schemas.sql`](configs/postgres_schemas.sql) before using
those accounts in a non-local environment. Docker only runs initialization
scripts automatically for a new, empty Postgres data volume.

For an existing database volume, apply the Gold schema migration explicitly:

```bash
docker compose exec -T postgres psql -U postgres -d football_db \
  -v ON_ERROR_STOP=1 < configs/migrations/002_gold_match_summary.sql
```

## Useful commands

```bash
# Follow service logs
docker compose logs -f producer
docker compose logs -f airflow-scheduler

# Stop containers (retains database and Grafana volumes)
docker compose down

# Also remove persisted Postgres and Grafana data
docker compose down -v
```

`docker compose down -v` deletes the named database and Grafana volumes. The
project's local Delta files under `data/` are bind-mounted and are not removed
by that command.

## Project layout

```text
airflow/dags/                 Scheduled Gold aggregation and Bronze maintenance
configs/                      Postgres schemas and migrations
src/football_etl/             API producer and transformation code
src/football_etl/spark/       Bronze, Silver, Gold, and JDBC Spark jobs
data/                         Local Delta data and streaming checkpoints
docker-compose.yml            Local service orchestration
Dockerfile.*                  Producer, Spark, and Airflow images
requirements.txt              Python producer dependencies
```
