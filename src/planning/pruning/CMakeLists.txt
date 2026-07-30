add_library(iceberg_planning_pruning OBJECT iceberg_predicate.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_pruning>
    PARENT_SCOPE)
