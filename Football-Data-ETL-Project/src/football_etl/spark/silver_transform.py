"""Delta bronze -> Postgres silver.

This is transform.py, ported from pandas to PySpark so it can run as a
continuous streaming job instead of a one-shot batch script. Same feature
engineering (match_year, total_goals, goal_difference, outcome), now
applied per micro-batch via foreachBatch.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from football_etl.config import BRONZE_PATH, SILVER_CHECKPOINT, SILVER_TABLE
from football_etl.spark.loading import write_append

match_event_schema = StructType([
    StructField("match_id", StringType()),
    StructField("date", TimestampType()),
    StructField("home_team", StringType()),
    StructField("away_team", StringType()),
    StructField("home_score", IntegerType()),
    StructField("away_score", IntegerType()),
])

spark = (
    SparkSession.builder.appName("silver_transform")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

parsed_df = (
    bronze_stream
    .withColumn("parsed", from_json(col("raw_value"), match_event_schema))
    .select("parsed.*")
    .filter(col("match_id").isNotNull())
    .dropDuplicates(["match_id", "date"])
)

transformed_df = (
    parsed_df
    .withColumn("match_year", year("date"))
    .withColumn("match_month", month("date"))
    .withColumn("day_of_week", date_format("date", "EEEE"))
    .withColumn("total_goals", col("home_score") + col("away_score"))
    .withColumn("goal_difference", col("home_score") - col("away_score"))
    .withColumn(
        "outcome",
        when(col("home_score") > col("away_score"), "Home Win")
        .when(col("home_score") < col("away_score"), "Away Win")
        .otherwise("Draw"),
    )
)


def write_batch(batch_df, batch_id):
    write_append(batch_df, SILVER_TABLE)


query = (
    transformed_df.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
