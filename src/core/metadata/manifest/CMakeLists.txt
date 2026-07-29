add_library(
  iceberg_core_metadata_manifest OBJECT
  iceberg_avro_codec.cpp iceberg_manifest.cpp iceberg_manifest_list.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_core_metadata_manifest>
    PARENT_SCOPE)
