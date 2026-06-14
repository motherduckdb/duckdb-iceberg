
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthTokenExchangeRequest::OAuthTokenExchangeRequest() {
}

OAuthTokenExchangeRequest OAuthTokenExchangeRequest::FromJSON(yyjson_val *obj) {
	OAuthTokenExchangeRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthTokenExchangeRequest OAuthTokenExchangeRequest::Copy() const {
	OAuthTokenExchangeRequest res;
	res.grant_type = grant_type;
	res.subject_token = subject_token;
	res.subject_token_type = subject_token_type.Copy();
	if (scope.has_value()) {
		res.scope.emplace();
		(*res.scope) = (*scope);
	}
	if (requested_token_type.has_value()) {
		res.requested_token_type.emplace();
		(*res.requested_token_type) = (*requested_token_type).Copy();
	}
	if (actor_token.has_value()) {
		res.actor_token.emplace();
		(*res.actor_token) = (*actor_token);
	}
	if (actor_token_type.has_value()) {
		res.actor_token_type.emplace();
		(*res.actor_token_type) = (*actor_token_type).Copy();
	}
	return res;
}

string OAuthTokenExchangeRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto grant_type_val = yyjson_obj_get(obj, "grant_type");
	if (!grant_type_val) {
		return "OAuthTokenExchangeRequest required property 'grant_type' is missing";
	} else {
		if (yyjson_is_str(grant_type_val)) {
			grant_type = yyjson_get_str(grant_type_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'grant_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(grant_type_val));
		}
	}
	auto subject_token_val = yyjson_obj_get(obj, "subject_token");
	if (!subject_token_val) {
		return "OAuthTokenExchangeRequest required property 'subject_token' is missing";
	} else {
		if (yyjson_is_str(subject_token_val)) {
			subject_token = yyjson_get_str(subject_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'subject_token' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(subject_token_val));
		}
	}
	auto subject_token_type_val = yyjson_obj_get(obj, "subject_token_type");
	if (!subject_token_type_val) {
		return "OAuthTokenExchangeRequest required property 'subject_token_type' is missing";
	} else {
		error = subject_token_type.TryFromJSON(subject_token_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto scope_val = yyjson_obj_get(obj, "scope");
	if (scope_val) {
		string scope_tmp;
		if (yyjson_is_str(scope_val)) {
			scope_tmp = yyjson_get_str(scope_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'scope_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(scope_val));
		}
		scope = std::move(scope_tmp);
	}
	auto requested_token_type_val = yyjson_obj_get(obj, "requested_token_type");
	if (requested_token_type_val) {
		TokenType requested_token_type_tmp;
		error = requested_token_type_tmp.TryFromJSON(requested_token_type_val);
		if (!error.empty()) {
			return error;
		}
		requested_token_type = std::move(requested_token_type_tmp);
	}
	auto actor_token_val = yyjson_obj_get(obj, "actor_token");
	if (actor_token_val) {
		string actor_token_tmp;
		if (yyjson_is_str(actor_token_val)) {
			actor_token_tmp = yyjson_get_str(actor_token_val);
		} else {
			return StringUtil::Format(
			    "OAuthTokenExchangeRequest property 'actor_token_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(actor_token_val));
		}
		actor_token = std::move(actor_token_tmp);
	}
	auto actor_token_type_val = yyjson_obj_get(obj, "actor_token_type");
	if (actor_token_type_val) {
		TokenType actor_token_type_tmp;
		error = actor_token_type_tmp.TryFromJSON(actor_token_type_val);
		if (!error.empty()) {
			return error;
		}
		actor_token_type = std::move(actor_token_type_tmp);
	}
	return "";
}

void OAuthTokenExchangeRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: grant_type
	yyjson_mut_obj_add_str(doc, obj, "grant_type", grant_type.c_str());

	// Serialize: subject_token
	yyjson_mut_obj_add_str(doc, obj, "subject_token", subject_token.c_str());

	// Serialize: subject_token_type
	yyjson_mut_val *subject_token_type_val = subject_token_type.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "subject_token_type", subject_token_type_val);

	// Serialize: scope
	if (scope.has_value()) {
		auto &scope_value = *scope;
		yyjson_mut_obj_add_str(doc, obj, "scope", scope_value.c_str());
	}

	// Serialize: requested_token_type
	if (requested_token_type.has_value()) {
		auto &requested_token_type_value = *requested_token_type;
		yyjson_mut_val *requested_token_type_value_val = requested_token_type_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "requested_token_type", requested_token_type_value_val);
	}

	// Serialize: actor_token
	if (actor_token.has_value()) {
		auto &actor_token_value = *actor_token;
		yyjson_mut_obj_add_str(doc, obj, "actor_token", actor_token_value.c_str());
	}

	// Serialize: actor_token_type
	if (actor_token_type.has_value()) {
		auto &actor_token_type_value = *actor_token_type;
		yyjson_mut_val *actor_token_type_value_val = actor_token_type_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "actor_token_type", actor_token_type_value_val);
	}
}

yyjson_mut_val *OAuthTokenExchangeRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
