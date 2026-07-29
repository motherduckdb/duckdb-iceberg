add_library(iceberg_catalog_rest_catalog_entry_table OBJECT
            iceberg_table_entry.cpp iceberg_table_information.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_catalog_rest_catalog_entry_table>
    PARENT_SCOPE)
