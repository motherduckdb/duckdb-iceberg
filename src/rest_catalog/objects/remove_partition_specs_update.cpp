
#include "rest_catalog/objects/remove_partition_specs_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::FromJSON(yyjson_val *obj) {
	RemovePartitionSpecsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemovePartitionSpecsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto spec_ids_val = yyjson_obj_get(obj, "spec-ids");
	if (!spec_ids_val) {
		return "RemovePartitionSpecsUpdate required property 'spec-ids' is missing";
	} else {
		if (yyjson_is_arr(spec_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(spec_ids_val, idx, max, val) {
				int32_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format(
					    "RemovePartitionSpecsUpdate property 'tmp' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				spec_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "RemovePartitionSpecsUpdate property 'spec_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(spec_ids_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "RemovePartitionSpecsUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *RemovePartitionSpecsUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: spec-ids
	yyjson_mut_val *spec_ids_arr = yyjson_mut_arr(doc);
	for (const auto &item : spec_ids) {
		yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
		yyjson_mut_arr_append(spec_ids_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "spec-ids", spec_ids_arr);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
