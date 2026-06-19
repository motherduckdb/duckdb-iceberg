CREATE or REPLACE TABLE default.pyspark_iceberg_table_v1 TBLPROPERTIES (
	'format-version'='1'
) AS SELECT * FROM parquet_file_view;

update default.pyspark_iceberg_table_v1
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

insert into default.pyspark_iceberg_table_v1
select * FROM default.pyspark_iceberg_table_v1
where l_extendedprice_double < 30000;

update default.pyspark_iceberg_table_v1
set l_orderkey_bool = not l_orderkey_bool;

update default.pyspark_iceberg_table_v1
set l_orderkey_bool = false
where l_partkey_int % 4 = 0;

update default.pyspark_iceberg_table_v1
set l_orderkey_bool = false
where l_partkey_int % 5 = 0;

ALTER TABLE default.pyspark_iceberg_table_v1
	ADD COLUMN schema_evol_added_col_1 INT;

UPDATE default.pyspark_iceberg_table_v1
	SET schema_evol_added_col_1 = 42;

UPDATE default.pyspark_iceberg_table_v1
SET schema_evol_added_col_1 = l_partkey_int
WHERE l_partkey_int % 5 = 0;

ALTER TABLE default.pyspark_iceberg_table_v1
ALTER COLUMN schema_evol_added_col_1 TYPE BIGINT;
