
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoveSnapshotRefUpdate {
public:
	RemoveSnapshotRefUpdate();
	RemoveSnapshotRefUpdate(const RemoveSnapshotRefUpdate &) = delete;
	RemoveSnapshotRefUpdate &operator=(const RemoveSnapshotRefUpdate &) = delete;
	RemoveSnapshotRefUpdate(RemoveSnapshotRefUpdate &&) = default;
	RemoveSnapshotRefUpdate &operator=(RemoveSnapshotRefUpdate &&) = default;

public:
	// Deserialization
	static RemoveSnapshotRefUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemoveSnapshotRefUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	string ref_name;
};

} // namespace rest_api_objects
} // namespace duckdb
