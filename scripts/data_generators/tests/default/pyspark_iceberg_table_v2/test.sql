CREATE or REPLACE TABLE default.pyspark_iceberg_table_v2 TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
) AS SELECT * FROM parquet_file_view;

update default.pyspark_iceberg_table_v2
set l_orderkey_bool=NULL,
    l_partkey_int=NULL,
    l_suppkey_long=NULL,
    l_extendedprice_float=NULL,
    l_extendedprice_double=NULL,
    l_shipdate_date=NULL,
    l_partkey_time=NULL,
    l_commitdate_timestamp=NULL,
    l_commitdate_timestamp_tz=NULL,
    l_comment_string=NULL,
    l_comment_blob=NULL
where l_partkey_int % 2 = 0;

insert into default.pyspark_iceberg_table_v2
select * FROM default.pyspark_iceberg_table_v2
where l_extendedprice_double < 30000;

update default.pyspark_iceberg_table_v2
set l_orderkey_bool = not l_orderkey_bool;

delete
from default.pyspark_iceberg_table_v2
where l_extendedprice_double < 10000;

delete
from default.pyspark_iceberg_table_v2
where l_extendedprice_double > 70000;

ALTER TABLE default.pyspark_iceberg_table_v2
	ADD COLUMN schema_evol_added_col_1 INT;

UPDATE default.pyspark_iceberg_table_v2
	SET schema_evol_added_col_1 = 42;

UPDATE default.pyspark_iceberg_table_v2
SET schema_evol_added_col_1 = l_partkey_int
WHERE l_partkey_int % 5 = 0;

ALTER TABLE default.pyspark_iceberg_table_v2
ALTER COLUMN schema_evol_added_col_1 TYPE BIGINT;
