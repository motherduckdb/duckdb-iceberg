
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Object2 {
public:
	// Deserialization
	static Object2 FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string operation;
	case_insensitive_map_t<string> additional_properties;
};

class Snapshot {
public:
	// Deserialization
	static Snapshot FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	int64_t snapshot_id;
	int64_t timestamp_ms;
	string manifest_list;
	Object2 summary;
	int64_t parent_snapshot_id;
	bool has_parent_snapshot_id = false;
	int64_t sequence_number;
	bool has_sequence_number = false;
	int64_t first_row_id;
	bool has_first_row_id = false;
	int32_t schema_id;
	bool has_schema_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
