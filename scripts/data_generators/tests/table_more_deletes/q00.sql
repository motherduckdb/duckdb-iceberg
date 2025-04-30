CREATE OR REPLACE TABLE default.table_more_deletes (
     dt     date,
     number integer,
     letter string
 )
 USING iceberg
 TBLPROPERTIES (
     'write.delete.mode'='merge-on-read',
     'write.update.mode'='merge-on-read',
     'write.merge.mode'='merge-on-read',
     'format-version'='2'
 );