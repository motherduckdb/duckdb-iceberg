INSERT INTO default.filtering_on_partition_bounds
SELECT 3 as seq, id AS col1 FROM range(2000, 3000);