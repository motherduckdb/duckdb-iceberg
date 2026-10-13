add_library(iceberg_function_copy OBJECT iceberg_copy_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_function_copy>
    PARENT_SCOPE)
