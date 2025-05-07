INSERT INTO default.filtering_on_partition_bounds
SELECT 2 as seq, id AS col1 FROM range(1000, 2000);