"""Delta bronze -> Postgres silver.

This is transform.py, ported from pandas to PySpark so it can run as a
continuous streaming job instead of a one-shot batch script. Same feature
engineering (match_year, total_goals, goal_difference, outcome), now
applied per micro-batch via foreachBatch.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    year,
    month,
    date_format,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

from football_etl.config import BRONZE_PATH, SILVER_CHECKPOINT, SILVER_TABLE
from football_etl.spark.loading import write_append


# ---------------------------------------------------------
# 1. Define the schema of the Football API JSON
# ---------------------------------------------------------

match_event_schema = StructType([
    StructField("id", IntegerType()),
    StructField("utcDate", TimestampType()),

    StructField(
        "homeTeam",
        StructType([
            StructField("name", StringType())
        ])
    ),

    StructField(
        "awayTeam",
        StructType([
            StructField("name", StringType())
        ])
    ),

    StructField(
        "score",
        StructType([
            StructField(
                "fullTime",
                StructType([
                    StructField("home", IntegerType()),
                    StructField("away", IntegerType()),
                ])
            )
        ])
    ),
])


# ---------------------------------------------------------
# 2. Create Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("silver_transform")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)


# ---------------------------------------------------------
# 3. Read Bronze Delta as a streaming DataFrame
# ---------------------------------------------------------

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


# ---------------------------------------------------------
# 4. Parse the raw JSON and flatten the nested structure
# ---------------------------------------------------------

parsed_df = (
    bronze_stream
    .withColumn(
        "parsed",
        from_json(col("raw_value"), match_event_schema)
    )
    .select(
        col("parsed.id").cast("string").alias("match_id"),
        col("parsed.utcDate").alias("date"),
        col("parsed.homeTeam.name").alias("home_team"),
        col("parsed.awayTeam.name").alias("away_team"),
        col("parsed.score.fullTime.home").alias("home_score"),
        col("parsed.score.fullTime.away").alias("away_score"),
    )
    .filter(col("match_id").isNotNull())
)


# ---------------------------------------------------------
# 5. Feature engineering
# ---------------------------------------------------------

transformed_df = (
    parsed_df

    # Date features
    .withColumn("match_year", year("date"))
    .withColumn("match_month", month("date"))
    .withColumn("day_of_week", date_format("date", "EEEE"))

    # Total goals
    # If the match has not been played, scores are NULL,
    # so total_goals should remain NULL.
    .withColumn(
        "total_goals",
        when(
            col("home_score").isNull() | col("away_score").isNull(),
            None,
        ).otherwise(
            col("home_score") + col("away_score")
        ),
    )

    # Goal difference
    # Also remains NULL for unplayed fixtures.
    .withColumn(
        "goal_difference",
        when(
            col("home_score").isNull() | col("away_score").isNull(),
            None,
        ).otherwise(
            col("home_score") - col("away_score")
        ),
    )

    # Match outcome
    .withColumn(
        "outcome",
        when(
            col("home_score").isNull() | col("away_score").isNull(),
            "Not Played",
        )
        .when(
            col("home_score") > col("away_score"),
            "Home Win",
        )
        .when(
            col("home_score") < col("away_score"),
            "Away Win",
        )
        .otherwise("Draw"),
    )
)


# ---------------------------------------------------------
# 6. Write each micro-batch to PostgreSQL Silver
# ---------------------------------------------------------

def write_batch(batch_df, batch_id):
    write_append(
        batch_df,
        SILVER_TABLE,
    )


# ---------------------------------------------------------
# 7. Start the streaming query
# ---------------------------------------------------------

query = (
    transformed_df.writeStream
    .foreachBatch(write_batch)
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT,
    )
    .trigger(
        processingTime="5 seconds"
    )
    .start()
)


# ---------------------------------------------------------
# 8. Keep the streaming job running
# ---------------------------------------------------------

query.awaitTermination()