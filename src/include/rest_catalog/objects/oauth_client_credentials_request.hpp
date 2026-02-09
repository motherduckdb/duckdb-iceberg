
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthClientCredentialsRequest {
public:
	// Deserialization
	static OAuthClientCredentialsRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string grant_type;
	string client_id;
	string client_secret;
	string scope;
	bool has_scope = false;
};

} // namespace rest_api_objects
} // namespace duckdb
