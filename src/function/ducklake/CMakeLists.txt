add_library(
  iceberg_function_ducklake OBJECT
  ducklake_column.cpp
  ducklake_column_stats.cpp
  ducklake_data_file.cpp
  ducklake_delete_file.cpp
  ducklake_partition.cpp
  ducklake_partition_column.cpp
  ducklake_schema.cpp
  ducklake_snapshot.cpp
  ducklake_table.cpp
  iceberg_to_ducklake.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_function_ducklake>
    PARENT_SCOPE)
