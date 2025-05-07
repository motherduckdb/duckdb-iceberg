CREATE or REPLACE TABLE default.filtering_on_bounds (
	col1 integer
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);