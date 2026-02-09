
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class MapType {
public:
	// Deserialization
	static MapType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string type;
	int32_t key_id;
	unique_ptr<Type> key;
	int32_t value_id;
	unique_ptr<Type> value;
	bool value_required;
};

} // namespace rest_api_objects
} // namespace duckdb
