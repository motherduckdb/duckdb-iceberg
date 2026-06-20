CREATE or REPLACE TABLE default.schema_evolve_float_to_double (
	col float
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_float_to_double
VALUES
	(1.23),
	(4.56),
	(7.89);

ALTER TABLE default.schema_evolve_float_to_double
	ALTER COLUMN col TYPE DOUBLE;

INSERT INTO default.schema_evolve_float_to_double
VALUES
	(1.23456789),
	(3.141592653589793),
	(2.718281828459045);
