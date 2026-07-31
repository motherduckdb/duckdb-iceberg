add_library(
  iceberg_core_expression OBJECT
  iceberg_hash.cpp iceberg_metrics.cpp iceberg_predicate_stats.cpp
  iceberg_transform.cpp iceberg_value.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_expression>
    PARENT_SCOPE)
