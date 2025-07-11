#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "storage/iceberg_table_requirement.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct AddSchemaUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SCHEMA;

	explicit AddSchemaUpdate(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {};
	// rest_api_objects::TableUpdate CreateAddSchemaUpdate();
};

struct AssertCreateRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_CREATE;

	explicit AssertCreateRequirement(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	// rest_api_objects::TableUpdate CreateAddAssertCreateRequirement();
};

struct AssignUUIDUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ASSIGN_UUID;

	explicit AssignUUIDUpdate(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
	// rest_api_objects::TableUpdate CreateAssignUUIDUpdate();
};

// struct AddSortOrder : public IcebergTableUpdate {
// 	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SORT_ORDER;
//
// 	explicit AddSortOrder(IcebergTableInformation &table_info);
// 	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {};
// 	rest_api_objects::TableUpdate CreateAddSortOrderUpdate();
// };
//

//
// struct SetCurrentSchema : public IcebergTableUpdate {
// 	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_CURRENT_SCHEMA;
//
// 	explicit SetCurrentSchema(IcebergTableInformation &table_info);
// 	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {};
// 	rest_api_objects::TableUpdate CreateSetCurrentSchemaUpdate();
// };
//
// struct SetDefaultSortOrder : public IcebergTableUpdate {
// 	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER;
//
// 	explicit SetDefaultSortOrder(IcebergTableInformation &table_info);
// 	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {};
// 	rest_api_objects::TableUpdate CreateSetDefaultOrderUpdate();
// };
//
// struct SetDefaultSpec : public IcebergTableUpdate {
// 	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SPEC;
// 	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {};
// 	rest_api_objects::TableUpdate CreateSetDefaultSpecUpdate();
// };

} // namespace duckdb
