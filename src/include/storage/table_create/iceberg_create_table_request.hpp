#pragma once
#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergTableInformation;
class ICTableEntry;

// TODO: can this just be an add snapshot?
struct IcebergCreateTableRequest {

	IcebergCreateTableRequest(shared_ptr<IcebergTableSchema> schema, string table_name);

public:
	void CreateManifest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	static shared_ptr<IcebergTableSchema> CreateIcebergSchema(const ICTableEntry *table_entry);
	rest_api_objects::CreateTableRequest CreateUpdateCreateTableRequest();
	void CreateCreateTableRequest(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	string CreateTableToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object);

private:
	string table_name;
	shared_ptr<IcebergTableSchema> initial_schema;
};

} // namespace duckdb
