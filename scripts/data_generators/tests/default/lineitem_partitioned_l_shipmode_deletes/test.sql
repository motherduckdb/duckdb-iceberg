CREATE OR REPLACE TABLE default.lineitem_partitioned_l_shipmode_deletes
USING iceberg
PARTITIONED BY (l_shipmode)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
)
as select * from parquet_file_view;

UPDATE default.lineitem_partitioned_l_shipmode_deletes
Set l_comment=NULL,
    l_quantity=NULL,
    l_discount=NULL,
    l_linestatus=NULL
where l_linenumber = 3 or l_linenumber = 4 or l_linenumber = 5;
