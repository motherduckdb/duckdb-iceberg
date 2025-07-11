#include "storage/table_update/generic.hpp"

namespace duckdb {
AddSchemaUpdate::AddSchemaUpdate(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
}

AddAssertCreateRequirement::AddAssertCreateRequirement(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
}

void AddAssertCreateRequirement::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                              IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_create.type.value = "assert-create";
	req.has_assert_create = true;
}

} // namespace duckdb
