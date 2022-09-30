# %%
import os
import time
from dataclasses import replace

import pyspark
from delta import *
from pandas.io.formats import string
from pyspark import Row
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, date_format, regexp_replace, concat, format_string, split, sha2
from pyspark.sql.functions import sum

WAREHOUSE_DIR = "spark-warehouse"
STAGED_TABLE = "stagedTable"
OUTPUT_TABLE = "aggTable"
INPUT_FILE = f"{WAREHOUSE_DIR}/MOCK_DATA.csv"
DELTA_FILE = f"{WAREHOUSE_DIR}/{STAGED_TABLE}"
OUTPUT_FILE = f"{WAREHOUSE_DIR}/{OUTPUT_TABLE}"
GRANULARITY = '"trade_date", "security"'
CURRENT_TIME = time.time()

"""
@Assumptions
1. The following columns must be unique in the input data. 
 - <trade_date, book, security> 
 This means that in a given book for a given date, there cannot be multiple positions for the same security.
"""


def upsert_data(source_df, delta_t):
    """
    @brief This method upsert the data in delta table.
            1. Inserts new rows.
            2. Updates existing rows.
    @Note Unfortunately, the current delta's merge implementation does not support delete from target table for
        the corresponding missing rows in source. Hopefully the community will implement that feature in 2023.
        This will also be a feature parity with standard SQL merge that supports `WHEN MATCHED BY [SOURCE|TARGET]`
    """
    delta_t.alias("t") \
        .merge(
        source_df.alias("s"),
        "s.key = t.key "
    ) \
        .whenMatchedUpdate(
        "t.change_key <> s.change_key",
        set={
            "quantity": "s.quantity",
            "price": "s.price",
            "last_updated": "s.last_updated",
            "change_key": "s.change_key"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

    print("+"*80)
    delta_table.history(1).select(col("operationMetrics")).foreach(print)


def delete_missing_data(source_df, delta_t):
    remove_rows = delta_t.alias("t").toDF().join(source_df, on="key", how='ANTI')
    delta_t.alias("t").merge(remove_rows.alias("s"), "t.key=s.key").whenMatchedDelete().execute()
    print("-"*80)
    delta_table.history(1).select(col("operationMetrics")).foreach(print)


def transform_date(dt: Column) -> Column:
    return regexp_replace(dt, "/", "-")


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# spark.sparkContext.setLogLevel("INFO")

updatesDF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(INPUT_FILE) \
    .withColumn("last_updated", lit(CURRENT_TIME)) \
    .withColumn("trade_date", transform_date(col("trade_date"))).alias("trade_date")\
    .withColumn("key", sha2(concat(col("trade_date"), col("book"), col("security")), 256))\
    .withColumn("change_key", sha2(concat(col("quantity"), col("price")), 256))\
    .drop_duplicates()

exists = os.path.exists(DELTA_FILE)
if not exists:
    print("delta table does not exist - creating a new one.")
    updatesDF.write.format("delta").mode("overwrite").saveAsTable(STAGED_TABLE)
    exit(0)

delta_table = DeltaTable.forPath(spark, path=DELTA_FILE)
delete_missing_data(updatesDF, delta_table)
# print("-"*80)
# delta_table.toDF().filter("security='Morgan Stanley'").show(10)
upsert_data(updatesDF, delta_table)
# print("+"*80)
# delta_table.toDF().filter("security='Morgan Stanley'").show(10)


"""
 - Final agg table saved as parquet (not delta). 
 - Read data from staged table (delta) and aggregate all security data by date
"""
spark.table(STAGED_TABLE) \
    .groupBy("trade_date", "security") \
    .agg(sum("quantity").alias("total_quantity"),
         sum(col("price") * col("quantity")).alias("notional")) \
    .withColumn("average", col("notional") / col("total_quantity")) \
    .drop("quantity", "price", "notional") \
    .write.format("parquet").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(OUTPUT_FILE)

df = spark.read.format("parquet").load(OUTPUT_FILE)
df.filter("security='Morgan Stanley'").show()
