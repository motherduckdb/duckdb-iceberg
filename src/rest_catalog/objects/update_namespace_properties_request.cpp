
#include "rest_catalog/objects/update_namespace_properties_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesRequest UpdateNamespacePropertiesRequest::FromJSON(yyjson_val *obj) {
	UpdateNamespacePropertiesRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string UpdateNamespacePropertiesRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto removals_val = yyjson_obj_get(obj, "removals");
	if (removals_val) {
		has_removals = true;
		if (yyjson_is_arr(removals_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				removals.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesRequest property 'removals' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removals_val));
		}
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (updates_val) {
		has_updates = true;
		if (yyjson_is_obj(updates_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(updates_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				updates.emplace(key_str, std::move(tmp));
			}
		} else {
			return "UpdateNamespacePropertiesRequest property 'updates' is not of type 'object'";
		}
	}
	return "";
}

yyjson_mut_val *UpdateNamespacePropertiesRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: removals
	if (has_removals) {
		yyjson_mut_val *removals_arr = yyjson_mut_arr(doc);
		for (const auto &item : removals) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(removals_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "removals", removals_arr);
	}

	// Serialize: updates
	if (has_updates) {
		yyjson_mut_val *updates_obj = yyjson_mut_obj(doc);
		for (const auto &it : updates) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, updates_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "updates", updates_obj);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
