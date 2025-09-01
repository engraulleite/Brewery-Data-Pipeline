import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.logger import setup_logger
from pathlib import Path
import json
from datetime import datetime

logger = setup_logger()

def read_bronze_json(spark, input_path: str):
    json_files = list(Path(input_path).glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {input_path}")

    try:
        df = spark.read.option("multiline", "true").json(str(input_path))
        return df
    except Exception as e:
        raise RuntimeError(f"Error reading JSON files in Bronze: {e}")

def main():
    data_process = os.getenv("data_process")
    if not data_process:
        data_process = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"data_process not defined. Using current date: {data_process}")

    logger.info(f"Checking Bronze for data_process = {data_process}")

    spark = SparkSession.builder \
        .appName("Verify Bronze") \
        .getOrCreate()

    input_path = f"/home/project/data/bronze/{data_process}"

    try:
        df = read_bronze_json(spark, input_path)

        # Check for expected columns
        expected_columns = ['id', 'name', 'brewery_type', 'state']
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Expected columns missing in Bronze: {missing_cols}")
        else:
            logger.info("All expected columns are present in Bronze.")

        # Check for duplicates in the 'id' column
        duplicates = df.groupBy("id").count().filter(col("count") > 1)
        num_duplicates = duplicates.count()
        if num_duplicates > 0:
            logger.warning(f"Found {num_duplicates} duplicates in Bronze.")
            duplicates.show(truncate=False)
        else:
            logger.info("No duplicates found in Bronze.")

        logger.info(f"Total records in Bronze: {df.count()}")
        logger.info("Verification OK: verify_bronze.py")

    except Exception as e:
        logger.error(f"Error in Bronze pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()