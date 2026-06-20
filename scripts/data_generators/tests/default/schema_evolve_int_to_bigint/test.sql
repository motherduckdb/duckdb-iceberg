CREATE or REPLACE TABLE default.schema_evolve_int_to_bigint (
	col integer
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);

INSERT INTO default.schema_evolve_int_to_bigint
VALUES
	(-2147483648),
	(-1),
	(0),
	(1),
	(2147483647);

ALTER TABLE default.schema_evolve_int_to_bigint ALTER col TYPE BIGINT;

INSERT INTO default.schema_evolve_int_to_bigint
VALUES
	(-9223372036854775808),
	(-1),
	(0),
	(1),
	(9223372036854775807);
