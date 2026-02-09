
#include "rest_catalog/objects/assign_uuidupdate.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssignUUIDUpdate AssignUUIDUpdate::FromJSON(yyjson_val *obj) {
	AssignUUIDUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssignUUIDUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto uuid_val = yyjson_obj_get(obj, "uuid");
	if (!uuid_val) {
		return "AssignUUIDUpdate required property 'uuid' is missing";
	} else {
		if (yyjson_is_str(uuid_val)) {
			uuid = yyjson_get_str(uuid_val);
		} else {
			return StringUtil::Format("AssignUUIDUpdate property 'uuid' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(uuid_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format("AssignUUIDUpdate property 'action' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *AssignUUIDUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: uuid
	yyjson_mut_obj_add_str(doc, obj, "uuid", uuid.c_str());

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
