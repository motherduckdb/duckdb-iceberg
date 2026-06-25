CREATE or REPLACE TABLE default.filtering_on_partition_bounds (
	seq integer,
	col1 integer
)
USING ICEBERG
PARTITIONED BY (seq)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

INSERT INTO default.filtering_on_partition_bounds
SELECT 1 as seq, id AS col1 FROM range(0, 1000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 2 as seq, id AS col1 FROM range(1000, 2000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 3 as seq, id AS col1 FROM range(2000, 3000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 4 as seq, id AS col1 FROM range(3000, 4000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 5 as seq, id AS col1 FROM range(4000, 5000);
