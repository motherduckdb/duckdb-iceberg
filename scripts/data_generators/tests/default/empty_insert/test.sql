CREATE or REPLACE TABLE default.empty_insert (
     col1 date,
     col2 integer,
     col3 string
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

INSERT INTO default.empty_insert
SELECT DATE '2010-06-11' col1, 42 col2, 'test' col3 WHERE 1 = 0
