add_subdirectory(copy)
add_subdirectory(merge_into)

add_library(
  iceberg_execution_operator OBJECT
  iceberg_delete.cpp iceberg_insert.cpp iceberg_update.cpp
  physical_iceberg_create_table.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_execution_operator>
    PARENT_SCOPE)
