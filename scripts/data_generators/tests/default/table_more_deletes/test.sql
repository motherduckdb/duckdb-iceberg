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

INSERT INTO default.table_more_deletes VALUES
	(CAST('2023-03-01' AS date), 1, 'a'),
	(CAST('2023-03-02' AS date), 2, 'b'),
	(CAST('2023-03-03' AS date), 3, 'c'),
	(CAST('2023-03-04' AS date), 4, 'd'),
	(CAST('2023-03-05' AS date), 5, 'e'),
	(CAST('2023-03-06' AS date), 6, 'f'),
	(CAST('2023-03-07' AS date), 7, 'g'),
	(CAST('2023-03-08' AS date), 8, 'h'),
	(CAST('2023-03-09' AS date), 9, 'i'),
	(CAST('2023-03-10' AS date), 10, 'j'),
	(CAST('2023-03-11' AS date), 11, 'k'),
	(CAST('2023-03-12' AS date), 12, 'l');

Delete from default.table_more_deletes
where number > 3 and number < 10;
