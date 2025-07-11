#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"
#include "storage/iceberg_table_update.hpp"
#include "storage/iceberg_table_requirement.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergCreateTableRequest;

struct IcebergTransactionData {
public:
	IcebergTransactionData(ClientContext &context, IcebergTableInformation &table_info)
	    : context(context), table_info(table_info), create(nullptr) {
	}

public:
	void AddSnapshot(IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&data_files);
	// add a schema update for a table
	void TableAddSchema();
	// add assert create requirement for a table
	void TableAddAssertCreate();
	// assign a UUID to the table
	void TableAssignUUID();

public:
	ClientContext &context;
	IcebergTableInformation &table_info;
	unique_ptr<IcebergCreateTableRequest> create;
	vector<unique_ptr<IcebergTableUpdate>> updates;
	vector<unique_ptr<IcebergTableRequirement>> requirements;

	//! Every insert/update/delete creates an alter of the table data
	vector<reference<IcebergAddSnapshot>> alters;
};

} // namespace duckdb
