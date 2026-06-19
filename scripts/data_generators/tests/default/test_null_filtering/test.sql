CREATE or REPLACE TABLE default.test_null_filtering (a int, b VARCHAR(20))
TBLPROPERTIES (
	'format-version' = '2',
	'write.delete.mode' = 'merge-on-read',
	'write.update.mode' = 'merge-on-read'
);

insert into default.test_null_filtering values
	(0, 'hello'),
	(1, 'world'),
	(null, 'duck'),
	(3, null);

insert into default.test_null_filtering values
	(5, 'wonderful'),
	(6, 'ducks');

insert into default.test_null_filtering values
	(null, null);

insert into default.test_null_filtering values
    (10, 'no nulls');
