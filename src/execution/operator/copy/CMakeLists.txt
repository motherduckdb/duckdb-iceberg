add_library(iceberg_execution_operator_copy OBJECT iceberg_copy.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_execution_operator_copy>
    PARENT_SCOPE)
