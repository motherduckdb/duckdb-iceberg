add_library(iceberg_core_metadata_puffin OBJECT iceberg_puffin_metadata.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_puffin>
    PARENT_SCOPE)
