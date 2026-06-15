#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/client_context.hpp"
#include "maintenance/table_lock_registry.hpp"

namespace duckdb {

struct IcebergTableInformation;

//! Resolve catalog.schema.table and load the latest Iceberg table metadata.
//! The function name is only used for actionable error messages.
IcebergTableInformation &LoadIcebergTable(ClientContext &context, const MaintenanceTableKey &key,
                                          const string &function_name);
shared_ptr<IcebergTableInformation> LoadIcebergTableShared(ClientContext &context, const MaintenanceTableKey &key,
                                                           const string &function_name);
//! Load metadata into a new table-information instance instead of reusing an
//! already-filled catalog entry.
shared_ptr<IcebergTableInformation> ReloadIcebergTableShared(ClientContext &context, const MaintenanceTableKey &key,
                                                             const string &function_name);

} // namespace duckdb
