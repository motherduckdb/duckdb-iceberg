
#include "rest_catalog/objects/get_namespace_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

GetNamespaceResponse GetNamespaceResponse::FromJSON(yyjson_val *obj) {
	GetNamespaceResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string GetNamespaceResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _namespace_val = yyjson_obj_get(obj, "namespace");
	if (!_namespace_val) {
		return "GetNamespaceResponse required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_null(properties_val)) {
			//! do nothing, property is explicitly nullable
		} else if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "GetNamespaceResponse property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "GetNamespaceResponse property 'properties' is not of type 'object'";
		}
	}
	return "";
}

yyjson_mut_val *GetNamespaceResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: namespace
	yyjson_mut_val *_namespace_val = _namespace.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "namespace", _namespace_val);

	// Serialize: properties
	if (has_properties) {
		yyjson_mut_val *properties_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, properties_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_obj);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
