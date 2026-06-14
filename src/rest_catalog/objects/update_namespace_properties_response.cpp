
#include "rest_catalog/objects/update_namespace_properties_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesResponse::UpdateNamespacePropertiesResponse() {
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::FromJSON(yyjson_val *obj) {
	UpdateNamespacePropertiesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::Copy() const {
	UpdateNamespacePropertiesResponse res;
	res.updated.reserve(updated.size());
	for (auto &item : updated) {
		res.updated.emplace_back(item);
	}
	res.removed.reserve(removed.size());
	for (auto &item : removed) {
		res.removed.emplace_back(item);
	}
	if (has_missing) {
		res.missing.reserve(missing.size());
		for (auto &item : missing) {
			res.missing.emplace_back(item);
		}
	}
	res.has_missing = has_missing;
	return res;
}

string UpdateNamespacePropertiesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto updated_val = yyjson_obj_get(obj, "updated");
	if (!updated_val) {
		return "UpdateNamespacePropertiesResponse required property 'updated' is missing";
	} else {
		if (yyjson_is_arr(updated_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updated_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesResponse property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				updated.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'updated' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(updated_val));
		}
	}
	auto removed_val = yyjson_obj_get(obj, "removed");
	if (!removed_val) {
		return "UpdateNamespacePropertiesResponse required property 'removed' is missing";
	} else {
		if (yyjson_is_arr(removed_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removed_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesResponse property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				removed.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'removed' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removed_val));
		}
	}
	auto missing_val = yyjson_obj_get(obj, "missing");
	if (missing_val) {
		has_missing = true;
		if (yyjson_is_null(missing_val)) {
			//! do nothing, property is explicitly nullable
		} else if (yyjson_is_arr(missing_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(missing_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "UpdateNamespacePropertiesResponse property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				missing.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'missing' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(missing_val));
		}
	}
	return "";
}

yyjson_mut_val *UpdateNamespacePropertiesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: updated
	yyjson_mut_val *updated_arr = yyjson_mut_arr(doc);
	for (const auto &item : updated) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(updated_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "updated", updated_arr);

	// Serialize: removed
	yyjson_mut_val *removed_arr = yyjson_mut_arr(doc);
	for (const auto &item : removed) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(removed_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "removed", removed_arr);

	// Serialize: missing
	if (has_missing) {
		yyjson_mut_val *missing_arr = yyjson_mut_arr(doc);
		for (const auto &item : missing) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(missing_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "missing", missing_arr);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
