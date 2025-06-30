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
class ICTableEntry;

// TODO: can this just be an addsnapshot?
struct IcebergCreateTableRequest {

	IcebergCreateTableRequest(ClientContext &context, IcebergTableInformation &table_info);

public:
	void CreateManifest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	rest_api_objects::CreateTableRequest CreateUpdateCreateTableRequest();
	void CreateCreateTableRequest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	static string CreateTableToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object, ICTableEntry &table_entry);
};

} // namespace duckdb
