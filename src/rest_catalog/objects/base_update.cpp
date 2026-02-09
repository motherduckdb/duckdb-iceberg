
#include "rest_catalog/objects/base_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

BaseUpdate BaseUpdate::FromJSON(yyjson_val *obj) {
	BaseUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string BaseUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	auto action_val = yyjson_obj_get(obj, "action");
	if (!action_val) {
		return "BaseUpdate required property 'action' is missing";
	} else {
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format("BaseUpdate property 'action' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *BaseUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: action
	yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
