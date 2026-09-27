"""Postgres silver -> Postgres gold. Batch job, triggered by Airflow on a schedule."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum, when

from football_etl.config import GOLD_TABLE, SILVER_TABLE
from football_etl.spark.loading import read_table, write_overwrite


def run(spark):
  silver_df = read_table(spark, SILVER_TABLE)

  # 1. Perspective when the team plays at Home
  home_perspective = silver_df.select(
      col("home_team").alias("team"),
      col("match_id"),
      when(col("outcome") == "Home Win", 3)
      .when(col("outcome") == "Draw", 1)
      .otherwise(0)
      .alias("points"),
      when(col("outcome") == "Home Win", 1).otherwise(0).alias("wins"),
      when(col("outcome") == "Away Win", 1).otherwise(0).alias("losses"),
      when(col("outcome") == "Draw", 1).otherwise(0).alias("draws"),
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
      when(col("outcome") == "Away Win", 1).otherwise(0).alias("wins"),
      when(col("outcome") == "Home Win", 1).otherwise(0).alias("losses"),
      when(col("outcome") == "Draw", 1).otherwise(0).alias("draws"),
      col("total_goals"),
      (col("goal_difference") * -1).alias(
          "goal_difference"
      ),  # Invert for away team
  )

  # 3. Combine both home and away records together
  all_teams_df = home_perspective.unionByName(away_perspective)

  # 4. Aggregate to build the final Gold team standings table
  gold_df = all_teams_df.groupBy("team").agg(
      count("match_id").cast("integer").alias("matches_played"),
      spark_sum("wins").cast("integer").alias("total_wins"),
      spark_sum("losses").cast("integer").alias("total_losses"),
      spark_sum("draws").cast("integer").alias("total_draws"),
      spark_sum("points").cast("integer").alias("total_points"),
      avg("total_goals").cast("double").alias("avg_match_goals"),
      avg("goal_difference").cast("double").alias("avg_goal_difference"),
  )

  write_overwrite(gold_df, GOLD_TABLE)


if __name__ == "__main__":
  spark = SparkSession.builder.appName("gold_aggregate").getOrCreate()
  try:
    run(spark)
  finally:
    spark.stop()
