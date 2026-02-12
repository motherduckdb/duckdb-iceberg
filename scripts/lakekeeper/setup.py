import requests, jwt
# from IPython.display import JSON
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas as pd

CATALOG_URL = "http://lakekeeper:8181/catalog"
MANAGEMENT_URL = "http://lakekeeper:8181/management"
KEYCLOAK_TOKEN_URL = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"

# 1.1 Sign In
# Login to Keycloak
CLIENT_ID = "spark"
CLIENT_SECRET = "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

SPARK_VERSION = pyspark.__version__
SPARK_MINOR_VERSION = '.'.join(SPARK_VERSION.split('.')[:2])
ICEBERG_VERSION = "1.10.0"

response = requests.post(
    url=KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "lakekeeper"
    },
    headers={"Content-type": "application/x-www-form-urlencoded"},
)
response.raise_for_status()
access_token = response.json()['access_token']

# Lets inspect the token we got to see that our application name is available:
# JSON(jwt.decode(access_token, options={"verify_signature": False}))

response = requests.get(
    url=f"{MANAGEMENT_URL}/v1/info",
    headers={"Authorization": f"Bearer {access_token}"},
)
response.raise_for_status()
# JSON(response.json())
# On first launch it shows "bootstrapped": False

# 1.2 Bootstrap
response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/bootstrap",
    headers={
        "Authorization": f"Bearer {access_token}"
    },
    json={
        "accept-terms-of-use": True,
        # Optionally, we can override the name / type of the user:
        # "user-email": "user@example.com",
        # "user-name": "Roald Amundsen",
        # "user-type": "human"
    },
)
response.raise_for_status()

# 1.3. Grant access to UI user

# Users will show up in the /v1/user endpoint after the first login via the UI
# or the first call to the /catalog/v1/config endpoint.
response = requests.get(
    url=f"{MANAGEMENT_URL}/v1/user",
    headers={"Authorization": f"Bearer {access_token}"},
)
response.raise_for_status()
# JSON(response.json())

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/permissions/server/assignments",
    headers={"Authorization": f"Bearer {access_token}"},
    json={
        "writes": [
            {
                "type": "admin",
                "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
            }
        ]
    }
)
response.raise_for_status()

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/permissions/project/assignments",
    headers={"Authorization": f"Bearer {access_token}"},
    json={
        "writes": [
            {
                "type": "project_admin",
                "user": "oidc~cfb55bf6-fcbb-4a1e-bfec-30c6649b52f8"
            }
        ]
    }
)
response.raise_for_status()

# 1.3.1 Grant Access to trino & duckdb & starrocks User

# First we login as the trino user, so that the user is known to
# Lakekeeper.

for client_id, client_secret in [("trino", "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ"), ("duckdb", "r2dHUlb7XrkSRcvrRqG5XZwQfnUS5NlL"), ("starrocks", "X5IWbfDJBTcU1F3PGZWgxDJwLyuFQmSf")]:
    response = requests.post(
        url=KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "lakekeeper"
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    access_token_client = response.json()['access_token']

    response = requests.post(
        url=f"{MANAGEMENT_URL}/v1/user",
        headers={"Authorization": f"Bearer {access_token_client}"},
        json={"update-if-exists": True}
    )
    response.raise_for_status()

# Users will show up in the /v1/user endpoint after the first login via the UI
# or the first call to the /catalog/v1/config endpoint.
response = requests.get(
    url=f"{MANAGEMENT_URL}/v1/user",
    headers={"Authorization": f"Bearer {access_token}"},
)
response.raise_for_status()
# JSON(response.json())

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/permissions/project/assignments",
    headers={"Authorization": f"Bearer {access_token}"},
    json={
        "writes": [
            {
                "type": "project_admin",
                "user": "oidc~94eb1d88-7854-43a0-b517-a75f92c533a5"
            },
            {
                "type": "project_admin",
                "user": "oidc~7515be4b-ce5b-4371-ab31-f40b97f74ec6"
            },
            {
                "type": "project_admin",
                "user": "oidc~7a5da0c5-24e2-4148-a8d9-71c748275928"
            }
        ]
    }
)
response.raise_for_status()

# 2 Creating a Warehouse

response = requests.post(
    url=f"{MANAGEMENT_URL}/v1/warehouse",
    headers={
        "Authorization": f"Bearer {access_token}"
    },
    json={
        "warehouse-name": "demo",
        "storage-profile": {
            "type": "s3",
            "bucket": "examples",
            "key-prefix": "initial-warehouse",
            "endpoint": "http://minio:9000",
            "region": "local-01",
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": True
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minio-root-user",
            "aws-secret-access-key": "minio-root-password"
        }
    }
)
response.raise_for_status()
# JSON(response.json())


# 3.1 Connect with Spark
conf = {
    "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR_VERSION}_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakekeeper.type": "rest",
    "spark.sql.catalog.lakekeeper.uri": CATALOG_URL,
    "spark.sql.catalog.lakekeeper.credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
    "spark.sql.catalog.lakekeeper.warehouse": WAREHOUSE,
    "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
    "spark.sql.catalog.lakekeeper.oauth2-server-uri": KEYCLOAK_TOKEN_ENDPOINT,
}

spark_config = SparkConf().setMaster('local').setAppName("Iceberg-REST")
for k, v in conf.items():
    spark_config = spark_config.set(k, v)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

spark.sql("USE lakekeeper")

# 3.2 Write table
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS my_namespace")
data = pd.DataFrame([[1, 'a-string', 2.2]], columns=['id', 'strings', 'floats'])
sdf = spark.createDataFrame(data)
sdf.writeTo(f"my_namespace.my_table").createOrReplace()