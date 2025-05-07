INSERT INTO default.filtering_on_partition_bounds
SELECT 5 as seq, id AS col1 FROM range(4000, 5000);