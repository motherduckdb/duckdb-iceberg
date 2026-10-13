add_library(
  iceberg_storage_statistics OBJECT
  iceberg_data_file_stats.cpp iceberg_statistics.cpp
  iceberg_variant_statistics.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_storage_statistics>
    PARENT_SCOPE)
