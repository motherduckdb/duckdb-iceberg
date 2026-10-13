CREATE or REPLACE TABLE default.lineitem_001_deletes
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view;

update default.lineitem_001_deletes
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
