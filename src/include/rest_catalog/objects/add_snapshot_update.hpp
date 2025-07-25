
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSnapshotUpdate {
public:
	AddSnapshotUpdate();
	AddSnapshotUpdate(const AddSnapshotUpdate &) = delete;
	AddSnapshotUpdate &operator=(const AddSnapshotUpdate &) = delete;
	AddSnapshotUpdate(AddSnapshotUpdate &&) = default;
	AddSnapshotUpdate &operator=(AddSnapshotUpdate &&) = default;

public:
	static AddSnapshotUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	Snapshot snapshot;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
