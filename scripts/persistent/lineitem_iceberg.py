from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

import sys
import os

DATA_GENERATION_DIR = '.'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.4.2.jar')

conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', SPARK_RUNTIME_PATH)
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("USE iceberg_catalog")

# Generate tpch 0.01

import pathlib
import tempfile
import duckdb

tempdir = pathlib.Path(tempfile.mkdtemp())
parquet_file = tempdir / "tmp.parquet"

duckdb_con = duckdb.connect()
duckdb_con.execute("call dbgen(sf=0.01)")
duckdb_con.execute(f"copy lineitem to '{parquet_file}' (FORMAT PARQUET)")

spark.read.parquet(parquet_file.as_posix()).createOrReplaceTempView('parquet_file_view')


spark.sql(
    """
CREATE or REPLACE TABLE lineitem_iceberg
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view;
"""
)

spark.sql(
    """
DELETE FROM lineitem_iceberg where l_extendedprice < 10000
"""
)
