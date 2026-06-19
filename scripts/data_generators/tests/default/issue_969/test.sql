CREATE OR REPLACE TABLE default.issue_969 (
  id INT,
  name STRING
) TBLPROPERTIES ('format-version'='2');

INSERT INTO default.issue_969 VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c'),
  (4, 'd'),
  (5, 'e');

ALTER TABLE default.issue_969 CREATE BRANCH side_branch;

DELETE FROM default.issue_969.branch_side_branch WHERE true;
