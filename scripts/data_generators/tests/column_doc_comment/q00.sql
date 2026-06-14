CREATE OR REPLACE TABLE default.column_doc_comment(
  id bigint COMMENT 'Primary identifier',
  population bigint COMMENT 'Resident count, 2024 census',
  name string
)
TBLPROPERTIES (
    'format-version'='2'
);
insert into default.column_doc_comment values
(1, 1000, 'alpha'),
(2, 2000, 'beta');
