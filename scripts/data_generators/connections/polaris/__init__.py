from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
import pyspark
import pyspark.sql

from ..base import IcebergConnection

import sys
import os

CONNECTION_KEY = 'polaris'


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self):
        super().__init__(CONNECTION_KEY, 'quickstart_catalog')
        self.con = self.get_connection()

    def get_connection(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
        )

        client_id = os.getenv('POLARIS_CLIENT_ID', '')
        client_secret = os.getenv('POLARIS_CLIENT_SECRET', '')
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "admin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

        if client_id == '' or client_secret == '':
            print("could not find client id or client secret to connect to polaris, aborting")
            return

        config = SparkConf()
        config.set(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19",
        )
        config.set('spark.sql.iceberg.vectorization.enabled', 'false')
        # Configure the 'polaris' catalog as an Iceberg rest catalog
        config.set("spark.sql.catalog.quickstart_catalog.type", "rest")
        config.set("spark.sql.catalog.quickstart_catalog", "org.apache.iceberg.spark.SparkCatalog")
        # Specify the rest catalog endpoint
        config.set("spark.sql.catalog.quickstart_catalog.uri", "http://localhost:8181/api/catalog")
        # Enable token refresh
        config.set("spark.sql.catalog.quickstart_catalog.token-refresh-enabled", "true")
        # specify the client_id:client_secret pair
        config.set("spark.sql.catalog.quickstart_catalog.credential", f"{client_id}:{client_secret}")
        # Set the warehouse to the name of the catalog we created
        config.set("spark.sql.catalog.quickstart_catalog.warehouse", "quickstart_catalog")
        # Scope set to PRINCIPAL_ROLE:ALL
        config.set("spark.sql.catalog.quickstart_catalog.scope", 'PRINCIPAL_ROLE:ALL')
        # Enable access credential delegation
        config.set("spark.sql.catalog.quickstart_catalog.header.X-Iceberg-Access-Delegation", 'vended-credentials')
        config.set("spark.sql.catalog.quickstart_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        config.set("spark.sql.catalog.quickstart_catalog.s3.region", "us-west-2")
        config.set("spark.history.fs.logDirectory", "/home/iceberg/spark-events")

        spark = SparkSession.builder.config(conf=config).getOrCreate()
        spark.sql("USE quickstart_catalog")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
