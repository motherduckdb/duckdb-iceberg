add_library(iceberg_core_metadata_snapshot OBJECT iceberg_snapshot.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_snapshot>
    PARENT_SCOPE)
