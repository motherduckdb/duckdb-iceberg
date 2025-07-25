
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetSnapshotRefUpdate {
public:
	SetSnapshotRefUpdate();
	SetSnapshotRefUpdate(const SetSnapshotRefUpdate &) = delete;
	SetSnapshotRefUpdate &operator=(const SetSnapshotRefUpdate &) = delete;
	SetSnapshotRefUpdate(SetSnapshotRefUpdate &&) = default;
	SetSnapshotRefUpdate &operator=(SetSnapshotRefUpdate &&) = default;

public:
	static SetSnapshotRefUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	SnapshotReference snapshot_reference;
	string ref_name;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
