"""Generic runner for OPTIMIZE/VACUUM maintenance SQL, called by Airflow's bronze_compaction_dag."""

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sql_block = sys.argv[1]
    spark = (
        SparkSession.builder.appName("run_sql")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    for statement in filter(None, (s.strip() for s in sql_block.split(";"))):
        spark.sql(statement)
    spark.stop()
