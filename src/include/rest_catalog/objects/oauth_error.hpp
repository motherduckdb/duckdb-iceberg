
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	OAuthError();
	OAuthError(const OAuthError &) = delete;
	OAuthError &operator=(const OAuthError &) = delete;
	OAuthError(OAuthError &&) = default;
	OAuthError &operator=(OAuthError &&) = default;

public:
	// Deserialization
	static OAuthError FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	OAuthError Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string _error;
	optional<string> error_description;
	optional<string> error_uri;
};

} // namespace rest_api_objects
} // namespace duckdb
