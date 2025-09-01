from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys
from src.logger import setup_logger

logger = setup_logger()
spark = SparkSession.builder \
    .appName("Check Duplicates - Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data_process = os.getenv("data_process")
if not data_process:
    logger.error("data_process was not defined.")
    sys.exit(1)

logger.info(f"data_process: {data_process}")

try:
    df = spark.read.format("delta") \
        .load("/home/project/data/silver") \
        .where(f"data_process = '{data_process}'")

    duplicate_count = df.groupBy("id").count().filter(col("count") > 1).count()

    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate records in Silver tier!")
    else:
        logger.info("No duplicate records found in Silver tier.")

except Exception as e:
    logger.error(f"Error checking duplicates in Silver: {e}")
    sys.exit(1)
finally:
    spark.stop()
