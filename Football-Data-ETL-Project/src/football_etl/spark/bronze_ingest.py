"""Kafka -> Delta bronze. Spark is the only Kafka consumer in this pipeline."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from football_etl.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, BRONZE_PATH, BRONZE_CHECKPOINT

spark = (
    SparkSession.builder.appName("bronze_ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_df = raw_df.select(
    col("key").cast("string").alias("match_id_key"),
    col("value").cast("string").alias("raw_value"),   # still raw JSON string - untouched
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingested_at"),
)

query = (
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .partitionBy("topic")
    .trigger(processingTime="5 seconds")
    .start(BRONZE_PATH)
)

query.awaitTermination()
