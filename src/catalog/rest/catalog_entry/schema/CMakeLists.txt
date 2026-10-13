add_library(iceberg_catalog_rest_catalog_entry_schema OBJECT
            iceberg_schema_entry.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:iceberg_catalog_rest_catalog_entry_schema>
    PARENT_SCOPE)
