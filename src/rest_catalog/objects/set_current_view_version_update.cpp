
#include "rest_catalog/objects/set_current_view_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::FromJSON(yyjson_val *obj) {
	SetCurrentViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetCurrentViewVersionUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto view_version_id_val = yyjson_obj_get(obj, "view-version-id");
	if (!view_version_id_val) {
		return "SetCurrentViewVersionUpdate required property 'view-version-id' is missing";
	} else {
		if (yyjson_is_int(view_version_id_val)) {
			view_version_id = yyjson_get_int(view_version_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentViewVersionUpdate property 'view_version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(view_version_id_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentViewVersionUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *SetCurrentViewVersionUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: view-version-id
	yyjson_mut_obj_add_int(doc, obj, "view-version-id", view_version_id);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
