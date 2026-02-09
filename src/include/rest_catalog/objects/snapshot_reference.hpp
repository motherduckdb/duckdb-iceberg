
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotReference {
public:
	// Deserialization
	static SnapshotReference FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string type;
	int64_t snapshot_id;
	int64_t max_ref_age_ms;
	bool has_max_ref_age_ms = false;
	int64_t max_snapshot_age_ms;
	bool has_max_snapshot_age_ms = false;
	int32_t min_snapshots_to_keep;
	bool has_min_snapshots_to_keep = false;
};

} // namespace rest_api_objects
} // namespace duckdb
