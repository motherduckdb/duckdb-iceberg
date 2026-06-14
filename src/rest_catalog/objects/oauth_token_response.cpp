
#include "rest_catalog/objects/oauth_token_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenResponse::OAuthTokenResponse() {
}

OAuthTokenResponse OAuthTokenResponse::FromJSON(yyjson_val *obj) {
	OAuthTokenResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenResponse OAuthTokenResponse::Copy() const {
	OAuthTokenResponse res;
	res.access_token = access_token;
	res.token_type = token_type;
	if (expires_in.has_value()) {
		res.expires_in.emplace();
		(*res.expires_in) = (*expires_in);
	}
	if (issued_token_type.has_value()) {
		res.issued_token_type.emplace();
		(*res.issued_token_type) = (*issued_token_type).Copy();
	}
	if (refresh_token.has_value()) {
		res.refresh_token.emplace();
		(*res.refresh_token) = (*refresh_token);
	}
	if (scope.has_value()) {
		res.scope.emplace();
		(*res.scope) = (*scope);
	}
	return res;
}

string OAuthTokenResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto access_token_val = yyjson_obj_get(obj, "access_token");
	if (!access_token_val) {
		return "OAuthTokenResponse required property 'access_token' is missing";
	} else {
		if (yyjson_is_str(access_token_val)) {
			access_token = yyjson_get_str(access_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'access_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(access_token_val));
		}
	}
	auto token_type_val = yyjson_obj_get(obj, "token_type");
	if (!token_type_val) {
		return "OAuthTokenResponse required property 'token_type' is missing";
	} else {
		if (yyjson_is_str(token_type_val)) {
			token_type = yyjson_get_str(token_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'token_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(token_type_val));
		}
	}
	auto expires_in_val = yyjson_obj_get(obj, "expires_in");
	if (expires_in_val) {
		int32_t expires_in_tmp;
		if (yyjson_is_int(expires_in_val)) {
			expires_in_tmp = yyjson_get_int(expires_in_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'expires_in_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(expires_in_val));
		}
		expires_in = std::move(expires_in_tmp);
	}
	auto issued_token_type_val = yyjson_obj_get(obj, "issued_token_type");
	if (issued_token_type_val) {
		TokenType issued_token_type_tmp;
		error = issued_token_type_tmp.TryFromJSON(issued_token_type_val);
		if (!error.empty()) {
			return error;
		}
		issued_token_type = std::move(issued_token_type_tmp);
	}
	auto refresh_token_val = yyjson_obj_get(obj, "refresh_token");
	if (refresh_token_val) {
		string refresh_token_tmp;
		if (yyjson_is_str(refresh_token_val)) {
			refresh_token_tmp = yyjson_get_str(refresh_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'refresh_token_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(refresh_token_val));
		}
		refresh_token = std::move(refresh_token_tmp);
	}
	auto scope_val = yyjson_obj_get(obj, "scope");
	if (scope_val) {
		string scope_tmp;
		if (yyjson_is_str(scope_val)) {
			scope_tmp = yyjson_get_str(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenResponse property 'scope_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(scope_val));
		}
		scope = std::move(scope_tmp);
	}
	return "";
}

void OAuthTokenResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: access_token
	yyjson_mut_obj_add_str(doc, obj, "access_token", access_token.c_str());

	// Serialize: token_type
	yyjson_mut_obj_add_str(doc, obj, "token_type", token_type.c_str());

	// Serialize: expires_in
	if (expires_in.has_value()) {
		auto &expires_in_value = *expires_in;
		yyjson_mut_obj_add_int(doc, obj, "expires_in", expires_in_value);
	}

	// Serialize: issued_token_type
	if (issued_token_type.has_value()) {
		auto &issued_token_type_value = *issued_token_type;
		yyjson_mut_val *issued_token_type_value_val = issued_token_type_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "issued_token_type", issued_token_type_value_val);
	}

	// Serialize: refresh_token
	if (refresh_token.has_value()) {
		auto &refresh_token_value = *refresh_token;
		yyjson_mut_obj_add_str(doc, obj, "refresh_token", refresh_token_value.c_str());
	}

	// Serialize: scope
	if (scope.has_value()) {
		auto &scope_value = *scope;
		yyjson_mut_obj_add_str(doc, obj, "scope", scope_value.c_str());
	}
}

yyjson_mut_val *OAuthTokenResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
