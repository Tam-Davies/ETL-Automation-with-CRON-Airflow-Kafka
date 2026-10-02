"""Postgres silver -> Postgres gold.

Batch job, triggered by Airflow on a schedule.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum as spark_sum,
    when,
)

from football_etl.config import GOLD_TABLE, SILVER_TABLE
from football_etl.spark.loading import read_table, write_overwrite


def run(spark):
    # Read only completed matches from Silver.
    # Future fixtures with outcome = "Not Played" are excluded.
    silver_df = (
        read_table(spark, SILVER_TABLE)
        .filter(col("outcome") != "Not Played")
    )

    # 1. Perspective when the team plays at Home
    home_perspective = silver_df.select(
        col("home_team").alias("team"),
        col("match_id"),
        when(col("outcome") == "Home Win", 3)
        .when(col("outcome") == "Draw", 1)
        .otherwise(0)
        .alias("points"),
        when(col("outcome") == "Home Win", 1)
        .otherwise(0)
        .alias("wins"),
        when(col("outcome") == "Away Win", 1)
        .otherwise(0)
        .alias("losses"),
        when(col("outcome") == "Draw", 1)
        .otherwise(0)
        .alias("draws"),
        col("total_goals"),
        col("goal_difference"),
    )

    # 2. Perspective when the team plays Away
    away_perspective = silver_df.select(
        col("away_team").alias("team"),
        col("match_id"),
        when(col("outcome") == "Away Win", 3)
        .when(col("outcome") == "Draw", 1)
        .otherwise(0)
        .alias("points"),
        when(col("outcome") == "Away Win", 1)
        .otherwise(0)
        .alias("wins"),
        when(col("outcome") == "Home Win", 1)
        .otherwise(0)
        .alias("losses"),
        when(col("outcome") == "Draw", 1)
        .otherwise(0)
        .alias("draws"),
        col("total_goals"),
        (col("goal_difference") * -1).alias(
            "goal_difference"
        ),
    )

    # 3. Combine Home and Away perspectives
    all_teams_df = home_perspective.unionByName(
        away_perspective
    )

    # 4. Aggregate team-level statistics
    gold_df = all_teams_df.groupBy("team").agg(
        count("match_id")
        .cast("integer")
        .alias("matches_played"),

        spark_sum("wins")
        .cast("integer")
        .alias("total_wins"),

        spark_sum("losses")
        .cast("integer")
        .alias("total_losses"),

        spark_sum("draws")
        .cast("integer")
        .alias("total_draws"),

        spark_sum("points")
        .cast("integer")
        .alias("total_points"),

        avg("total_goals")
        .cast("double")
        .alias("avg_match_goals"),

        avg("goal_difference")
        .cast("double")
        .alias("avg_goal_difference"),
    )

    # 5. Replace the existing Gold table with the latest aggregation
    write_overwrite(
        gold_df,
        GOLD_TABLE,
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("gold_aggregate")
        .getOrCreate()
    )

    try:
        run(spark)
    finally:
        spark.stop()