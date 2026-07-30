add_library(iceberg_planning_metadata_io_deletes OBJECT
            iceberg_deletes_file_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_metadata_io_deletes>
    PARENT_SCOPE)
