CREATE or REPLACE TABLE default.schema_evolve_widen_decimal (
	col DECIMAL(12, 8)
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_widen_decimal
VALUES
	(1234.12345678),
	(987.87654321),
	(12.12345678);

ALTER TABLE default.schema_evolve_widen_decimal 
	ALTER COLUMN col TYPE DECIMAL(18, 8);

INSERT INTO default.schema_evolve_widen_decimal
VALUES
	(1234567890.12345678),
	(987654321.98765432),
	(123.12345678);
