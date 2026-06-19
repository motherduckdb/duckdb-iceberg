CREATE OR REPLACE TABLE default.row_lineage_test (
  id INT,
  data STRING
)
TBLPROPERTIES (
  'format-version'='3'
);

INSERT INTO default.row_lineage_test VALUES
(1, 'a'),
(2, 'b'),
(3, 'c'),
(4, 'd'),
(5, 'e');

UPDATE default.row_lineage_test
SET data = CONCAT(data, '_u1')
WHERE id IN (2, 4);

DELETE FROM default.row_lineage_test
WHERE id IN (3, 5);

INSERT INTO default.row_lineage_test VALUES
(6, 'f'),
(7, 'g');

UPDATE default.row_lineage_test
SET data = 'replaced'
WHERE id IN (1, 6);

DELETE FROM default.row_lineage_test WHERE id = 7;

INSERT INTO default.row_lineage_test VALUES
(7, 'g_new');
