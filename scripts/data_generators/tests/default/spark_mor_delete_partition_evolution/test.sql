CREATE OR REPLACE TABLE default.spark_mor_delete_partition_evolution (
    id INT,
    ts TIMESTAMP,
    val STRING
)
USING ICEBERG
PARTITIONED BY (days(ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.spark_mor_delete_partition_evolution VALUES
    (1, TIMESTAMP '2021-05-01 09:00:00', 'a'),
    (2, TIMESTAMP '2021-05-01 10:30:00', 'b');

ALTER TABLE default.spark_mor_delete_partition_evolution
REPLACE PARTITION FIELD days(ts)
WITH hours(ts);

INSERT INTO default.spark_mor_delete_partition_evolution VALUES
    (3, TIMESTAMP '2021-06-02 11:00:00', 'c');

DELETE FROM default.spark_mor_delete_partition_evolution
WHERE val = 'b';
