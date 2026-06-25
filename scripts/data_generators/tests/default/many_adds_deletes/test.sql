CREATE or REPLACE TABLE default.many_adds_deletes
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view limit 10000;

update default.many_adds_deletes
set l_orderkey=NULL,
    l_partkey=NULL,
    l_suppkey=NULL,
    l_linenumber=NULL,
    l_quantity=NULL,
    l_extendedprice=NULL,
    l_discount=NULL,
    l_shipdate=NULL,
    l_comment=NULL
where l_partkey % 2 = 0;

INSERT INTO default.many_adds_deletes
SELECT * FROM parquet_file_view limit 10000;

update default.many_adds_deletes
set l_orderkey=NULL,
    l_partkey=NULL,
    l_suppkey=NULL,
    l_linenumber=NULL,
    l_quantity=NULL,
    l_extendedprice=NULL,
    l_discount=NULL,
    l_shipdate=NULL,
    l_comment=NULL
where l_partkey % 2 = 0;

INSERT INTO default.many_adds_deletes
SELECT * FROM parquet_file_view limit 10000;

update default.many_adds_deletes
set l_orderkey=NULL,
    l_partkey=NULL,
    l_suppkey=NULL,
    l_linenumber=NULL,
    l_quantity=NULL,
    l_extendedprice=NULL,
    l_discount=NULL,
    l_shipdate=NULL,
    l_comment=NULL
where l_partkey % 2 = 0;

INSERT INTO default.many_adds_deletes
SELECT * FROM parquet_file_view limit 10000;

update default.many_adds_deletes
set l_orderkey=NULL,
    l_partkey=NULL,
    l_suppkey=NULL,
    l_linenumber=NULL,
    l_quantity=NULL,
    l_extendedprice=NULL,
    l_discount=NULL,
    l_shipdate=NULL,
    l_comment=NULL
where l_partkey % 2 = 0;
