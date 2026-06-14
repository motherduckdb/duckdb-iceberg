
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoveSnapshotsUpdate {
public:
	RemoveSnapshotsUpdate();
	RemoveSnapshotsUpdate(const RemoveSnapshotsUpdate &) = delete;
	RemoveSnapshotsUpdate &operator=(const RemoveSnapshotsUpdate &) = delete;
	RemoveSnapshotsUpdate(RemoveSnapshotsUpdate &&) = default;
	RemoveSnapshotsUpdate &operator=(RemoveSnapshotsUpdate &&) = default;

public:
	// Deserialization
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemoveSnapshotsUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	vector<int64_t> snapshot_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
