add_subdirectory(metadata_io)
add_subdirectory(pruning)
add_subdirectory(snapshot)

add_library(
  iceberg_planning OBJECT
  iceberg_multi_file_list.cpp iceberg_multi_file_reader.cpp
  iceberg_optimizer.cpp iceberg_scan_plan_provider.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning>
    PARENT_SCOPE)
