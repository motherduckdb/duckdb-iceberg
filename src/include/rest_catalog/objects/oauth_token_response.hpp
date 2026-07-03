
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/token_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenResponse {
public:
	OAuthTokenResponse();
	OAuthTokenResponse(const OAuthTokenResponse &) = delete;
	OAuthTokenResponse &operator=(const OAuthTokenResponse &) = delete;
	OAuthTokenResponse(OAuthTokenResponse &&) = default;
	OAuthTokenResponse &operator=(OAuthTokenResponse &&) = default;

public:
	// Deserialization
	static OAuthTokenResponse FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	OAuthTokenResponse Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string access_token;
	string token_type;
	optional<int32_t> expires_in;
	optional<TokenType> issued_token_type;
	optional<string> refresh_token;
	optional<string> scope;
};

} // namespace rest_api_objects
} // namespace duckdb
