CREATE OR REPLACE TABLE default.truncate_partitioned_bigint_for_insert (
    id INTEGER,
    value BIGINT
)
USING iceberg
PARTITIONED BY (truncate(value, 10));

INSERT INTO default.truncate_partitioned_bigint_for_insert VALUES
    (1,  1),
    (2,  11),
    (3,  21),
    (4,  31),
    (5,  41),
    (6,  51),
    (7,  61),
    (8,  71),
    (9,  81),
    (10, 91),
    (11, NULL);
