CREATE or REPLACE TABLE default.test_table_properties (a int)
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read'
);

INSERT INTO default.test_table_properties VALUES
	(1),
	(2),
	(3);

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'read.split.target-size'='268435456',
    'foo'='baz'
);

ALTER TABLE default.test_table_properties UNSET TBLPROPERTIES ('foo');

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'duckdb_man'='superman'
);

INSERT INTO default.test_table_properties VALUES
	(1),
	(2),
	(3);

ALTER TABLE default.test_table_properties SET TBLPROPERTIES (
    'another1'='neato'
);
