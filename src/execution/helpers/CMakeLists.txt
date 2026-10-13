add_library(iceberg_execution_helpers OBJECT equality_delete_helpers.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_execution_helpers>
    PARENT_SCOPE)
