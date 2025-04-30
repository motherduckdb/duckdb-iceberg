insert into default.pyspark_iceberg_table_v1
select * FROM default.pyspark_iceberg_table_v1
where l_extendedprice_double < 30000