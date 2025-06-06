# README
this iceberg table is generated by using DuckDB (v0.7.0) to generated TPC-H lineitem
SF0.01 then storing that to a parquet file.

Then pyspark (3.3.1) was used with the iceberg extension from https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.0.0/iceberg-spark-runtime-3.3_2.12-1.0.0.jar
to write the iceberg table.

finally, using pyspark, a delete query was performed on this iceberg table:

```
DELETE FROM iceberg_catalog.lineitem_iceberg where l_extendedprice < 10000
```

The result for Q06 of TPC-H on this table according to pyspark is now:
```
[Row(revenue=Decimal('1077536.9101'))]
```

Note: it appears that there are no deletes present in this iceberg table, the whole thing was rewritten.
this is likely due to the fact that the table is so small?

### Regeneration

Follow the steps from `data/persistent/iceberg/lineitem_iceberg`, copy the final folder into `lineitem_iceberg_no_hint`
Remove the `version-hint.txt` and the `.crc` file for the version hint.
Finally update all the tests that refer to the internals of the rewritten metadata.
