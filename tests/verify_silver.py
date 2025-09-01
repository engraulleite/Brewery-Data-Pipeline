import os
from pyspark.sql import SparkSession
from src.logger import setup_logger
from datetime import datetime

logger = setup_logger()

def main():
    data_process = os.getenv("data_process")
    if not data_process:
        data_process = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"data_process not defined. Using current date: {data_process}")

    logger.info(f"Checking Silver for data_process = {data_process}")

    spark = SparkSession.builder \
        .appName("Verify Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        df = spark.read.format("delta").load("/home/project/data/silver").where(f"data_process = '{data_process}'")
        logger.info("Delta Lake reading in Silver layer completed successfully.")
        df.printSchema()
        df.show(5, truncate=False)

        expected_columns = [
            "id", "name", "state", "brewery_type", "data_process", "silver_load_date"
        ]
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Expected columns missing in Silver: {missing_cols}")
        else:
            logger.info("All expected columns are present in Silver.")

        total = df.count()
        logger.info(f"Total records in Silver ({data_process}): {total}")

        logger.info("Verification OK: verify_silver.py")

    except Exception as e:
        logger.error(f"Error in Silver pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()