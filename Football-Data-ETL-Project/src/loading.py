import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from ingestion import extract_multiple_seasons as ems
from transform import transform_match_data as td

load_dotenv()

# Fetch PostgreSQL credentials from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create SQLAlchemy engine for PostgreSQL using psycopg2
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def load_to_db(df, table_name):
  df.to_sql(table_name, engine, if_exists="replace", index=False)
  print(f"Successfully loaded {len(df)} rows into table '{table_name}'.")
  return df


if __name__ == "__main__":
  # Run the full ETL pipeline
  raw_matches = ems([2023, 2024, 2025])
  clean_matches = td(raw_matches)
  load_to_db(clean_matches, "premier_league_matches")