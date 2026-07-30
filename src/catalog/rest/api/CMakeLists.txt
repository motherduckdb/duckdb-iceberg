add_library(
  iceberg_catalog_rest_api OBJECT
  api_utils.cpp
  catalog_api.cpp
  catalog_utils.cpp
  iceberg_add_snapshot.cpp
  iceberg_create_table_request.cpp
  iceberg_expression.cpp
  iceberg_manifest_merge.cpp
  iceberg_retry.cpp
  iceberg_scan_planning.cpp
  iceberg_table_update.cpp
  iceberg_type.cpp
  table_update.cpp
  url_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest_api>
    PARENT_SCOPE)
