#include "storage/table_update/generic.hpp"

namespace duckdb {
AddSchemaUpdate::AddSchemaUpdate(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
}

AssignUUID::AssignUUID(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
}

void AssignUUID::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.has_assign_uuidupdate = true;
	update.assign_uuidupdate.action = "assign-uuid";
	update.assign_uuidupdate.has_action = true;
	update.assign_uuidupdate.uuid = "TODO-fix-me";
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
