CREATE OR REPLACE TABLE default.year_timestamptz (
    partition_col TIMESTAMP,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (year(partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.year_timestamptz VALUES
    (TIMESTAMP '2020-05-15 14:30:45', 12345, 'click'),
    (TIMESTAMP '2021-08-22 09:15:20', 67890, 'purchase'),
    (TIMESTAMP '2022-03-10 11:45:30', 54321, 'view');
