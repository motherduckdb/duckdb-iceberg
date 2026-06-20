CREATE OR REPLACE TABLE default.row_lineage_test_upgraded (
  id INT,
  data STRING
)
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

INSERT INTO default.row_lineage_test_upgraded VALUES
(1, 'a'),
(2, 'b'),
(3, 'c'),
(4, 'd'),
(5, 'e');

UPDATE default.row_lineage_test_upgraded
SET data = CONCAT(data, '_u1')
WHERE id IN (2, 4);

DELETE FROM default.row_lineage_test_upgraded
WHERE id IN (3, 5);

INSERT INTO default.row_lineage_test_upgraded VALUES
(6, 'f'),
(7, 'g');

UPDATE default.row_lineage_test_upgraded
SET data = 'replaced'
WHERE id IN (1, 6);

DELETE FROM default.row_lineage_test_upgraded WHERE id = 7;

INSERT INTO default.row_lineage_test_upgraded VALUES
(7, 'g_new');

ALTER TABLE default.row_lineage_test_upgraded
SET TBLPROPERTIES (
	'format-version'='3',
	'write.delete.mode' = 'merge-on-read',
	'write.merge.mode' = 'merge-on-read',
	'write.update.mode' = 'merge-on-read'
);
