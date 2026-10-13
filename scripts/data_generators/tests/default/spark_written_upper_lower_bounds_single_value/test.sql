CREATE OR REPLACE TABLE default.spark_written_upper_lower_bounds_single_value (
    int_type INTEGER,
    bigint_type BIGINT,
    varchar_type VARCHAR(100),
    bool_type BOOLEAN,
    float_type FLOAT,
    double_type DOUBLE,
    decimal_type_18_3 DECIMAL(18, 3),
    date_type DATE,
    timestamp_type TIMESTAMP,
    binary_type BINARY
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '2132'
);

INSERT INTO default.spark_written_upper_lower_bounds_single_value
SELECT
    42,
    9223372036854775807,
    'constant_string',
    true,
    3.14159,
    2.718281828459045,
    123.456,
    DATE '2024-06-15',
    TIMESTAMP '2024-06-15 12:30:45.123456',
    X'FFFFFFFF'
FROM (SELECT explode(sequence(1, 1000)) AS dummy);
