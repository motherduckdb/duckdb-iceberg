from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import pyspark
import pyspark.sql

from ..base import IcebergConnection

import sys
import os

CONNECTION_KEY = 'lakekeeper'

SCRIPT_DIR = os.path.dirname(__file__)
DATA_GENERATION_DIR = os.path.join(SCRIPT_DIR, '..', '..', '..', 'data', 'generated', 'iceberg', 'spark-local')
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', '..', 'iceberg-spark-runtime-3.5_2.12-1.4.2.jar')

CATALOG_URL = "http://localhost:8181/catalog"
MANAGEMENT_URL = "http://localhost:8181/management"
KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"
WAREHOUSE = "demo"

CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.7.0"


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkLocal(IcebergConnection):
    def __init__(self):
        super().__init__(CONNECTION_KEY, 'lakekeeper')
        self.con = self.get_connection()

    def get_connection(self):
        conf = {
            "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakekeeper.type": "rest",
            "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
            "spark.sql.catalog.lakekeeper.credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
            "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
            "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_URL,
        }

        spark_config = SparkConf().setMaster('local').setAppName("Iceberg-REST")
        for k, v in conf.items():
            spark_config = spark_config.set(k, v)

        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

        spark.sql("USE lakekeeper")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
        spark.sql("USE NAMESPACE default")
        return spark
