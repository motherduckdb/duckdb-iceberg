
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
	// Deserialization
	static AddSnapshotUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	BaseUpdate base_update;
	Snapshot snapshot;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
