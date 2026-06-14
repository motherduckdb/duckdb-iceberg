
#include "rest_catalog/objects/set_default_sort_order_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetDefaultSortOrderUpdate::SetDefaultSortOrderUpdate() {
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::FromJSON(yyjson_val *obj) {
	SetDefaultSortOrderUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::Copy() const {
	SetDefaultSortOrderUpdate res;
	res.base_update = base_update.Copy();
	res.sort_order_id = sort_order_id;
	if (has_action) {
		res.action = action;
	}
	res.has_action = has_action;
	return res;
}

string SetDefaultSortOrderUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
	if (!sort_order_id_val) {
		return "SetDefaultSortOrderUpdate required property 'sort-order-id' is missing";
	} else {
		if (yyjson_is_int(sort_order_id_val)) {
			sort_order_id = yyjson_get_int(sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSortOrderUpdate property 'sort_order_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sort_order_id_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val && !yyjson_is_null(action_val)) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSortOrderUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *SetDefaultSortOrderUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: sort-order-id
	yyjson_mut_obj_add_int(doc, obj, "sort-order-id", sort_order_id);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
