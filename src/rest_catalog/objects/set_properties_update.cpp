
#include "rest_catalog/objects/set_properties_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetPropertiesUpdate SetPropertiesUpdate::FromJSON(yyjson_val *obj) {
	SetPropertiesUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetPropertiesUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "SetPropertiesUpdate required property 'updates' is missing";
	} else {
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
					    "SetPropertiesUpdate property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				updates.emplace(key_str, std::move(tmp));
			}
		} else {
			return "SetPropertiesUpdate property 'updates' is not of type 'object'";
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetPropertiesUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *SetPropertiesUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: BaseUpdate
	yyjson_mut_val *base_updatebase_obj = base_update.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(base_updatebase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize: updates
	yyjson_mut_val *updates_obj = yyjson_mut_obj(doc);
	for (const auto &[key, value] : updates) {
		yyjson_mut_obj_add_str(doc, updates_obj, key.c_str(), value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "updates", updates_obj);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
