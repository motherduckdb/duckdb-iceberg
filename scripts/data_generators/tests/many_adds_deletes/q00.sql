CREATE or REPLACE TABLE default.many_adds_deletes
       TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
       )
AS SELECT * FROM parquet_file_view limit 10000;
