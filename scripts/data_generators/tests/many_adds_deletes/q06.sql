INSERT INTO default.many_adds_deletes
SELECT * FROM parquet_file_view limit 10000;
