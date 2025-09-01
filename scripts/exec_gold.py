import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F

from src.transforms import (
    create_spark_session,
    write_delta
)
from src.utils import get_timezone_aware_date
from src.logger import logger


def extract_mode() -> str:
    """Extracts the load mode from environment variables."""
    return os.getenv("LOAD_MODE", "full").lower()


def extract_data_processs(mode: str) -> list:
    """
    Determines the processing dates based on the load mode by checking
    the available partition folders in the Silver layer.
    """
    base_silver_path = "/home/project/data/silver"

    if mode == "full":
        # In full mode, list all available date partitions from the silver path
        return sorted([
            name.split("=")[1]
            for name in os.listdir(base_silver_path)
            if os.path.isdir(os.path.join(base_silver_path, name)) and name.startswith("data_process=")
        ])
    elif mode == "delta":
        # In delta mode, process the last N days defined by DELTA_DAYS
        days = int(os.getenv("DELTA_DAYS", "1"))
        return [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(days)
        ]
    else:
        # For a specific date load
        return [os.getenv("data_process", get_timezone_aware_date())]


def main():
    try:
        spark = create_spark_session("Gold Aggregation")
        mode = extract_mode()
        logger.info(f"Load mode: {mode}")

        base_silver_path = "/home/project/data/silver"
        base_gold_path = "/home/project/data/gold"
        
        data_processs = extract_data_processs(mode)
        logger.info(f"Reading Silver table with a logical filter for dates: {data_processs}")

        df_silver = spark.read.format("delta").load(base_silver_path).where(F.col("data_process").isin(data_processs))

        # Validate that the necessary columns for aggregation exist
        required_columns = {"state", "brewery_type"}
        if not required_columns.issubset(set(df_silver.columns)):
            raise ValueError("Required columns are missing from the Silver layer: state, brewery_type")

        # Aggregate data to create the Gold DataFrame
        df_gold = df_silver.groupBy("state", "brewery_type").agg(
            F.count("*").alias("brewery_count")
        ).withColumn("data_process", F.lit(get_timezone_aware_date())) \
         .withColumn("gold_load_date", F.current_timestamp())

        write_delta(df_gold, base_gold_path, mode="append", partition_col="data_process")

        # Create or recreate the Gold table
        table_name = "gold_breweries"
        if mode == "full":
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{base_gold_path}'
        """)

        # Add comments to columns if it's a full load
        if mode == "full":
            logger.info("Adding comments to the Gold layer columns")
            spark.sql("ALTER TABLE gold_breweries ALTER COLUMN state COMMENT 'Brewery state'")
            spark.sql("ALTER TABLE gold_breweries ALTER COLUMN brewery_type COMMENT 'Type of brewery'")
            spark.sql("ALTER TABLE gold_breweries ALTER COLUMN brewery_count COMMENT 'Count of grouped breweries'")
            spark.sql("ALTER TABLE gold_breweries ALTER COLUMN data_process COMMENT 'Logical processing date'")
            spark.sql("ALTER TABLE gold_breweries ALTER COLUMN gold_load_date COMMENT 'Timestamp of the Gold load'")

        logger.info("Gold layer finished successfully.")
    except Exception as e:
        logger.error(f"Error executing the Gold layer: {e}")
        raise


if __name__ == "__main__":
    main()