add_library(
  iceberg_core_metadata_schema OBJECT
  iceberg_column_definition.cpp iceberg_field_mapping.cpp
  iceberg_table_schema.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_schema>
    PARENT_SCOPE)
