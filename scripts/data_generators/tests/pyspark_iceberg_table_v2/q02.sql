insert into default.pyspark_iceberg_table_v2
select * FROM default.pyspark_iceberg_table_v2
where l_extendedprice_double < 30000