update default.pyspark_iceberg_table_v1
set l_orderkey_bool = false
where l_partkey_int % 5 = 0;