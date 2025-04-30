CREATE OR REPLACE TABLE default.table_unpartitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg
;
