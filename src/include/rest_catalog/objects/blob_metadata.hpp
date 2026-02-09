
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BlobMetadata {
public:
	// Deserialization
	static BlobMetadata FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string type;
	int64_t snapshot_id;
	int64_t sequence_number;
	vector<int32_t> fields;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
