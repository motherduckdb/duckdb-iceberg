CREATE OR REPLACE TABLE default.spark_rewrite_mor_residual_deletes (
    id INT,
    category STRING,
    payload STRING
)
USING ICEBERG
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);
