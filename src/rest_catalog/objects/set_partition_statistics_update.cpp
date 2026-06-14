
#include "rest_catalog/objects/set_partition_statistics_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetPartitionStatisticsUpdate::SetPartitionStatisticsUpdate() {
}

SetPartitionStatisticsUpdate SetPartitionStatisticsUpdate::FromJSON(yyjson_val *obj) {
	SetPartitionStatisticsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetPartitionStatisticsUpdate SetPartitionStatisticsUpdate::Copy() const {
	SetPartitionStatisticsUpdate res;
	res.base_update = base_update.Copy();
	res.partition_statistics = partition_statistics.Copy();
	if (has_action) {
		res.action = action;
	}
	res.has_action = has_action;
	return res;
}

string SetPartitionStatisticsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
	if (!partition_statistics_val) {
		return "SetPartitionStatisticsUpdate required property 'partition-statistics' is missing";
	} else {
		error = partition_statistics.TryFromJSON(partition_statistics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val && !yyjson_is_null(action_val)) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetPartitionStatisticsUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *SetPartitionStatisticsUpdate::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: partition-statistics
	yyjson_mut_val *partition_statistics_val = partition_statistics.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "partition-statistics", partition_statistics_val);

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
