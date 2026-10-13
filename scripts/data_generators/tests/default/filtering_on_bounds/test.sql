CREATE or REPLACE TABLE default.filtering_on_bounds (
	col1 integer
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

INSERT INTO default.filtering_on_bounds
SELECT id AS col1 FROM range(0, 1000);

INSERT INTO default.filtering_on_bounds
SELECT id AS col1 FROM range(1000, 2000);

INSERT INTO default.filtering_on_bounds
SELECT id AS col1 FROM range(2000, 3000);

INSERT INTO default.filtering_on_bounds
SELECT id AS col1 FROM range(3000, 4000);

INSERT INTO default.filtering_on_bounds
SELECT id AS col1 FROM range(4000, 5000);
