INSERT INTO default.filtering_on_partition_bounds
SELECT 1 as seq, id AS col1 FROM range(0, 1000);