
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesResponse {
public:
	// Deserialization
	static UpdateNamespacePropertiesResponse FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	vector<string> updated;
	vector<string> removed;
	vector<string> missing;
	bool has_missing = false;
};

} // namespace rest_api_objects
} // namespace duckdb
