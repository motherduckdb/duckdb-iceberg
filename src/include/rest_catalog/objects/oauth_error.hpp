
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	// Deserialization
	static OAuthError FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string _error;
	string error_description;
	bool has_error_description = false;
	string error_uri;
	bool has_error_uri = false;
};

} // namespace rest_api_objects
} // namespace duckdb
