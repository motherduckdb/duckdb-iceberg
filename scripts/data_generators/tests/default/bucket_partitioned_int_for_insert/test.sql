CREATE OR REPLACE TABLE default.bucket_partitioned_int_for_insert (
    id INTEGER,
    value INTEGER
)
USING iceberg
PARTITIONED BY (bucket(4, value));

INSERT INTO default.bucket_partitioned_int_for_insert VALUES
    (1,  1),
    (2,  2),
    (3,  3),
    (4,  4),
    (5,  5),
    (6,  6),
    (7,  7),
    (8,  8),
    (9,  9),
    (10, 10),
    (11, NULL)
