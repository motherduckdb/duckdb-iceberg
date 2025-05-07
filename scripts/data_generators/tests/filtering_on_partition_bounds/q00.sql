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
