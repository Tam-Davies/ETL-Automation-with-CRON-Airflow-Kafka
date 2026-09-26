import json
from config import (
    CONSUMER_GROUP_POSTGRES,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_MATCHES,
)
from kafka import KafkaConsumer
import pandas as pd
from sqlalchemy import create_engine
from transform import transform_match_data as td

# Creatind database
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

consumer = KafkaConsumer(
    KAFKA_TOPIC_MATCHES,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=CONSUMER_GROUP_POSTGRES,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("--- Postgres Consumer Running: Saving transformed data to DB ---")
batch = []

for message in consumer:
  match_data = message.value
  batch.append(match_data)

  if len(batch) >= 10:
    df = pd.DataFrame(batch)
    df_transformed = td(df)
    df_transformed.to_sql(
        "premier_league_matches", engine, if_exists="append", index=False
    )
    print(f"Committed {len(batch)} records to PostgreSQL.")
    batch = []