import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from datetime import datetime
import pytz

def get_data_process():
    env_date = os.getenv("data_process")
    if env_date:
        return env_date
    tz = pytz.timezone("America/Sao_Paulo")
    return datetime.now(tz).strftime("%Y-%m-%d")

def create_spark_session():
    return SparkSession.builder         
    .appName("Check Duplicates - Gold")         
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")         
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")         
    .getOrCreate()

def main():
    spark = create_spark_session()
    data_process = get_data_process()
    print(f"data_process: {data_process}")

    df = spark.read.format("delta").load(f"/home/project/data/gold/{data_process}")
    df.cache()

    print("Checking for duplicates by ('state', 'brewery_type')")
    df.groupBy("state", "brewery_type").agg(count("*").alias("count")).filter("count > 1").show(10, truncate=False)

    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()