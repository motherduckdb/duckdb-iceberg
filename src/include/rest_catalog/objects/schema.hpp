
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Object1 {
public:
	// Deserialization
	static Object1 FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	int32_t schema_id;
	bool has_schema_id = false;
	vector<int32_t> identifier_field_ids;
	bool has_identifier_field_ids = false;
};

class Schema {
public:
	// Deserialization
	static Schema FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	StructType struct_type;
	Object1 object_1;
};

} // namespace rest_api_objects
} // namespace duckdb
