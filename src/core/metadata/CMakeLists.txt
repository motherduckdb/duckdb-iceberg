add_subdirectory(manifest)
add_subdirectory(partition)
add_subdirectory(puffin)
add_subdirectory(schema)
add_subdirectory(snapshot)
add_subdirectory(sort)

add_library(iceberg_core_metadata OBJECT iceberg_table_metadata.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata>
    PARENT_SCOPE)
