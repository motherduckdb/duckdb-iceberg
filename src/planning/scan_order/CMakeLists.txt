add_library(iceberg_planning_scan_order OBJECT iceberg_scan_order.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_planning_scan_order>
    PARENT_SCOPE)
