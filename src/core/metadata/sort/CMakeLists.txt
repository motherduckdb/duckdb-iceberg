add_library(iceberg_core_metadata_sort OBJECT iceberg_sort_order.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_sort>
    PARENT_SCOPE)
