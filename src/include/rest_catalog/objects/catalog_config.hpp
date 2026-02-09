
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CatalogConfig {
public:
	// Deserialization
	static CatalogConfig FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;
	vector<string> endpoints;
	bool has_endpoints = false;
};

} // namespace rest_api_objects
} // namespace duckdb
