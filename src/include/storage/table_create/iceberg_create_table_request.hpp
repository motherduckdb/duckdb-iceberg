#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergTableInformation;

// TODO: can this just be an addsnapshot?
struct IcebergCreateTableRequest {

public:
//	IcebergAddSnapshot(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
//					   const string &manifest_list_path, IcebergSnapshot &&snapshot);
	IcebergCreateTableRequest(ClientContext &context, IcebergTableInformation &table_info);

public:
	void CreateManifest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	rest_api_objects::CreateTableRequest CreateUpdateCreateTableRequest();
	void CreateCreateTableRequest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);


public:
	IcebergManifestFile manifest_file;
	IcebergManifestList manifest_list;
	IcebergManifest manifest;
	IcebergSnapshot snapshot;
};

} // namespace duckdb
