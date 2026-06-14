
#include "rest_catalog/objects/oauth_error.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

OAuthError::OAuthError() {
}

OAuthError OAuthError::FromJSON(yyjson_val *obj) {
	OAuthError res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

OAuthError OAuthError::Copy() const {
	OAuthError res;
	res._error = _error;
	if (error_description.has_value()) {
		res.error_description.emplace();
		(*res.error_description) = (*error_description);
	}
	if (error_uri.has_value()) {
		res.error_uri.emplace();
		(*res.error_uri) = (*error_uri);
	}
	return res;
}

string OAuthError::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _error_val = yyjson_obj_get(obj, "error");
	if (!_error_val) {
		return "OAuthError required property 'error' is missing";
	} else {
		if (yyjson_is_str(_error_val)) {
			_error = yyjson_get_str(_error_val);
		} else {
			return StringUtil::Format("OAuthError property '_error' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(_error_val));
		}
	}
	auto error_description_val = yyjson_obj_get(obj, "error_description");
	if (error_description_val) {
		string error_description_tmp;
		if (yyjson_is_str(error_description_val)) {
			error_description_tmp = yyjson_get_str(error_description_val);
		} else {
			return StringUtil::Format(
			    "OAuthError property 'error_description_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(error_description_val));
		}
		error_description = std::move(error_description_tmp);
	}
	auto error_uri_val = yyjson_obj_get(obj, "error_uri");
	if (error_uri_val) {
		string error_uri_tmp;
		if (yyjson_is_str(error_uri_val)) {
			error_uri_tmp = yyjson_get_str(error_uri_val);
		} else {
			return StringUtil::Format("OAuthError property 'error_uri_tmp' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(error_uri_val));
		}
		error_uri = std::move(error_uri_tmp);
	}
	return "";
}

void OAuthError::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: error
	yyjson_mut_obj_add_str(doc, obj, "error", _error.c_str());

	// Serialize: error_description
	if (error_description.has_value()) {
		auto &error_description_value = *error_description;
		yyjson_mut_obj_add_str(doc, obj, "error_description", error_description_value.c_str());
	}

	// Serialize: error_uri
	if (error_uri.has_value()) {
		auto &error_uri_value = *error_uri;
		yyjson_mut_obj_add_str(doc, obj, "error_uri", error_uri_value.c_str());
	}
}

yyjson_mut_val *OAuthError::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
