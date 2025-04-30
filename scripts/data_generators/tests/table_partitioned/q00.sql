CREATE OR REPLACE TABLE default.table_partitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg
PARTITIONED BY (days(dt))
