
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class StructField {
public:
	// Deserialization
	static StructField FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	int32_t id;
	string name;
	unique_ptr<Type> type;
	bool required;
	string _doc;
	bool has__doc = false;
	PrimitiveTypeValue initial_default;
	bool has_initial_default = false;
	PrimitiveTypeValue write_default;
	bool has_write_default = false;
};

} // namespace rest_api_objects
} // namespace duckdb
