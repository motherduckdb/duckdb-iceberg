CREATE OR REPLACE TABLE default.quickstart_table (
     id BIGINT, data STRING
)
USING ICEBERG;

INSERT INTO default.quickstart_table VALUES
	(1, 'some data'),
	(2, 'more data'),
	(3, 'yet more data');
