
#pragma once

#include "yyjson.hpp"
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
	OAuthTokenResponse(const OAuthTokenResponse&) = delete;
	OAuthTokenResponse& operator=(const OAuthTokenResponse&) = delete;
	OAuthTokenResponse(OAuthTokenResponse&&) = default;
	OAuthTokenResponse &operator=(OAuthTokenResponse&&) = default;
public:
	static OAuthTokenResponse FromJSON(yyjson_val *obj);
	OAuthTokenResponse Copy() const;
public:
	string TryFromJSON(yyjson_val *obj);
public:
	string access_token;
	string token_type;
	int32_t expires_in;
	bool has_expires_in;
	TokenType issued_token_type;
	bool has_issued_token_type;
	string refresh_token;
	bool has_refresh_token;
	string scope;
	bool has_scope;
};

} // namespace rest_api_objects
} // namespace duckdb

