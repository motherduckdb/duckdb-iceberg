add_library(iceberg_planning_metadata_io_manifest OBJECT
            bound_iceberg_manifest_entry.cpp iceberg_manifest_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_metadata_io_manifest>
    PARENT_SCOPE)
