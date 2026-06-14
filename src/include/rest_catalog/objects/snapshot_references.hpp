
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotReferences {
public:
	SnapshotReferences();
	SnapshotReferences(const SnapshotReferences &) = delete;
	SnapshotReferences &operator=(const SnapshotReferences &) = delete;
	SnapshotReferences(SnapshotReferences &&) = default;
	SnapshotReferences &operator=(SnapshotReferences &&) = default;

public:
	// Deserialization
	static SnapshotReferences FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	SnapshotReferences Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	case_insensitive_map_t<SnapshotReference> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
