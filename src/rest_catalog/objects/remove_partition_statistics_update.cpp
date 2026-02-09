
#include "rest_catalog/objects/remove_partition_statistics_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePartitionStatisticsUpdate RemovePartitionStatisticsUpdate::FromJSON(yyjson_val *obj) {
	RemovePartitionStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemovePartitionStatisticsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "RemovePartitionStatisticsUpdate required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "RemovePartitionStatisticsUpdate property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "RemovePartitionStatisticsUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *RemovePartitionStatisticsUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
