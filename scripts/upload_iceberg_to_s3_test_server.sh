#!/bin/bash
# FIXME: this path sounds incorrect? 'data/generated/iceberg/generated_0_01'
AWS_ACCESS_KEY_ID=duckdb_minio_admin AWS_SECRET_ACCESS_KEY=duckdb_minio_admin_password aws --endpoint-url http://duckdb-minio.com:9000 s3 sync data/generated/iceberg/generated_0_01 s3://test-bucket-public/iceberg_0_01
