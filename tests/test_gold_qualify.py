import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestGold") \
        .master("local[1]") \
        .getOrCreate()

def test_gold_aggregation_columns(spark):
    df = spark.read.format("delta").load("/home/project/data/gold")

    expected_columns = {"state", "brewery_type", "brewery_count", "data_process", "gold_load_date"}
    actual_columns = set(df.columns)

    assert expected_columns.issubset(actual_columns), f"Expected columns missing: {expected_columns - actual_columns}"

def test_gold_grouping_uniqueness(spark):
    df = spark.read.format("delta").load("/home/project/data/gold")

    duplicates = df.groupBy("state", "brewery_type", "data_process") \
                   .count() \
                   .filter(col("count") > 1)

    assert duplicates.count() == 0, "Duplicates found in Gold for state, brewery_type, and data_process"
