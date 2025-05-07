INSERT INTO default.filtering_on_partition_bounds
SELECT 4 as seq, id AS col1 FROM range(3000, 4000);