
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSchemaUpdate {
public:
	// Deserialization
	static AddSchemaUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	BaseUpdate base_update;
	Schema schema;
	string action;
	bool has_action = false;
	int32_t last_column_id;
	bool has_last_column_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
