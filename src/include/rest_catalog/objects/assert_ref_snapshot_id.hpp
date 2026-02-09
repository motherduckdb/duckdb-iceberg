
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_requirement_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertRefSnapshotId {
public:
	// Deserialization
	static AssertRefSnapshotId FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	TableRequirementType type;
	string ref;
	int64_t snapshot_id;
	bool has_snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
