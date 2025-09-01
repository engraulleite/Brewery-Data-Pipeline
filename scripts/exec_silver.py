import os
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException

from src.transforms import (
    create_spark_session,
    write_delta,
    list_available_dates,
    load_and_union_jsons,
    transform_to_silver
)
from src.utils import get_latest_date_folder, get_timezone_aware_date
from src.logger import CustomLogger

logger_instance = CustomLogger()
logger = logger_instance.get_logger()


def extract_mode() -> str:
    """Extracts the load mode from environment variables."""
    return os.getenv("LOAD_MODE", "full").lower()


def extract_data_processs(mode: str, base_path: str) -> list:
    """Determines the processing dates based on the load mode."""
    if mode == "full":
        return list_available_dates(base_path)
    elif mode == "delta":
        days = int(os.getenv("DELTA_DAYS", "1"))
        return [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(days)
        ]
    else:
        # For a specific date load
        return [os.getenv("data_process", get_timezone_aware_date())]


def create_table_if_not_exists(spark, path: str, recreate: bool = False):
    """
    Creates the Silver layer Delta table with the simple name: silver_breweries.
    This naming convention avoids conflicts with namespaces in local catalogs.
    """
    table_name = "silver_breweries"

    if recreate:
        try:
            if spark.catalog.tableExists(table_name):
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except AnalysisException:
            # Table does not exist, so no action is needed
            pass

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)

    if recreate:
        logger.info("Adding comments to the Silver layer columns")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN id COMMENT 'Brewery ID'")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN name COMMENT 'Brewery name'")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN state COMMENT 'Brewery state'")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN brewery_type COMMENT 'Type of brewery'")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN data_process COMMENT 'Logical processing date'")
        spark.sql("ALTER TABLE silver_breweries ALTER COLUMN silver_load_date COMMENT 'Timestamp of the Silver load'")


def main():
    try:
        spark = create_spark_session("Silver layer")
        mode = extract_mode()
        logger.info(f"Load mode: {mode}")

        base_bronze_path = "/home/project/data/bronze"
        base_silver_path = "/home/project/data/silver"

        if mode == "full":
            # For a full load, process only the latest available date in bronze
            latest_date = get_latest_date_folder(base_bronze_path)
            data_processs = [latest_date]
        else:
            data_processs = extract_data_processs(mode, base_bronze_path)

        logger.info(f"Silver processing dates: {data_processs}")

        # The input path is based on the first date to be processed
        input_path = os.path.join(base_bronze_path, data_processs[0])

        for date in data_processs:
            df_bronze = load_and_union_jsons(spark, input_path)
            df_silver = transform_to_silver(df_bronze, date)
            write_delta(df_silver, base_silver_path, mode="append", partition_col=["data_process", "state"])

        # Create or recreate the table after writing the data
        create_table_if_not_exists(spark, base_silver_path, recreate=(mode == "full"))
        logger.info("Silver layer finished successfully.")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()