CREATE or REPLACE TABLE default.type_promotion_partitioned_table (
	id int
)
PARTITIONED BY (id)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

insert into default.type_promotion_partitioned_table values
	(1),
	(1),
	(1),
	(1),
	(1),
	(2),
	(2),
	(2),
	(2),
	(2),
	(3),
	(3),
	(3),
	(4),
	(4),
	(4),
	(5),
	(5);

ALTER TABLE default.type_promotion_partitioned_table CHANGE COLUMN id id BIGINT;

insert into default.type_promotion_partitioned_table values
	(1),
	(1),
	(1),
	(2),
	(2),
	(2),
	(3),
	(3),
	(3),
	(4),
	(4),
	(4),
	(4),
	(4),
	(5),
	(5),
	(5),
	(5),
	(5),
	(6),
	(6),
	(6),
	(6),
	(6),
	(6),
	(6),
	(6),
	(7),
	(7),
	(7),
	(7),
	(7);
