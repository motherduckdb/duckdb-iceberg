CREATE OR REPLACE TABLE default.test_numeric_casting (
	byte_col TINYINT,
	short_col SMALLINT,
	int_col INT,
	long_col BIGINT
) USING ICEBERG;
