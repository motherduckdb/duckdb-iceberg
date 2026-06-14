
#include "rest_catalog/objects/oauth_token_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenRequest::OAuthTokenRequest() {
}

OAuthTokenRequest OAuthTokenRequest::FromJSON(yyjson_val *obj) {
	OAuthTokenRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenRequest OAuthTokenRequest::Copy() const {
	OAuthTokenRequest res;
	if (has_oauth_client_credentials_request) {
		res.oauth_client_credentials_request = oauth_client_credentials_request.Copy();
	}
	res.has_oauth_client_credentials_request = has_oauth_client_credentials_request;
	if (has_oauth_token_exchange_request) {
		res.oauth_token_exchange_request = oauth_token_exchange_request.Copy();
	}
	res.has_oauth_token_exchange_request = has_oauth_token_exchange_request;
	return res;
}

string OAuthTokenRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	error = oauth_client_credentials_request.TryFromJSON(obj);
	if (error.empty()) {
		has_oauth_client_credentials_request = true;
	}
	error = oauth_token_exchange_request.TryFromJSON(obj);
	if (error.empty()) {
		has_oauth_token_exchange_request = true;
	}
	if (!has_oauth_client_credentials_request && !has_oauth_token_exchange_request) {
		return "OAuthTokenRequest failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

void OAuthTokenRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (has_oauth_client_credentials_request) {
		oauth_client_credentials_request.PopulateJSON(doc, obj);
	} else if (has_oauth_token_exchange_request) {
		oauth_token_exchange_request.PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *OAuthTokenRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
