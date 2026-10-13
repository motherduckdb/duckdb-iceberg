add_library(
  iceberg_planning_metadata_io_manifest_list OBJECT
  bound_iceberg_manifest_list_entry.cpp iceberg_manifest_list_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_planning_metadata_io_manifest_list>
    PARENT_SCOPE)
