CREATE OR REPLACE TABLE default.bucket_partitioned_sparse_integer (
    id INTEGER,
    value INTEGER,
    name STRING
)
USING iceberg
PARTITIONED BY (bucket(16, value));

INSERT INTO default.bucket_partitioned_sparse_integer
VALUES
    (1,  1,  'alpha'),
    (2,  2,  'beta'),
    (3,  42, 'gamma'),
    (4,  1,  'alpha_dup'),
    (5,  42, 'gamma_dup')
