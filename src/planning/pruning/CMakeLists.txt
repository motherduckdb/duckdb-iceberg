add_library(
  iceberg_planning_pruning OBJECT iceberg_file_pruner.cpp iceberg_predicate.cpp
                                  iceberg_table_filter.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_pruning>
    PARENT_SCOPE)
