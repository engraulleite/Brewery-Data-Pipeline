import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from delta import configure_spark_with_delta_pip
from src.logger import setup_logger

logger = setup_logger()

builder = (
    SparkSession.builder.appName("VerifyGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

data_process = os.getenv("data_process")
if not data_process:
    logger.error("Environment variable data_process not defined.")
    sys.exit(1)

logger.info(f"Checking Gold table")

base_dir = Path(__file__).resolve().parents[1]
gold_path = base_dir / "data" / "gold"

if not gold_path.exists():
    logger.error(f"Directory not found: {gold_path}")
    sys.exit(1)

try:
    df = spark.read.format("delta").load(str(gold_path)).where(f"data_process = '{data_process}'")
    logger.info("Delta Lake reading in Gold layer completed successfully.")

    df.printSchema()
    df.show(5, truncate=False)

    count = df.count()
    logger.info(f"Total records in Gold ({data_process}): {count}")

except AnalysisException as e:
    logger.error(f"Error reading Gold Delta Lake: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    sys.exit(1)
finally:
    try:
        spark.stop()
    except NameError:
        pass