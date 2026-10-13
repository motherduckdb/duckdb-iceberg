add_subdirectory(avro)
add_subdirectory(deletes)
add_subdirectory(manifest)
add_subdirectory(manifest_list)

add_library(iceberg_planning_metadata_io OBJECT base_manifest_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_metadata_io>
    PARENT_SCOPE)
