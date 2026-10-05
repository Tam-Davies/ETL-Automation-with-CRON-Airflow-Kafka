"""Shared write helpers for the Spark jobs. Not run standalone -
imported into silver_transform.py and gold_aggregate.py."""

from football_etl.config import PG_URL, PG_PROPS


def write_append(df, table: str):
    df.write.mode("append").jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def write_overwrite(df, table: str):
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(url=PG_URL, table=table, properties=PG_PROPS)
    )


def read_table(spark, table: str):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)
