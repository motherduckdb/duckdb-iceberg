
#include "rest_catalog/objects/completed_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Object5 Object5::FromJSON(yyjson_val *obj) {
	Object5 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Object5::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "Object5 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

yyjson_mut_val *Object5::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: status
	yyjson_mut_val *status_val = status.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "status", status_val);

	return obj;
}

CompletedPlanningResult CompletedPlanningResult::FromJSON(yyjson_val *obj) {
	CompletedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CompletedPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_5.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

yyjson_mut_val *CompletedPlanningResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: ScanTasks
	yyjson_mut_val *scan_tasksbase_obj = scan_tasks.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(scan_tasksbase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize base class: Object5
	yyjson_mut_val *object_5base_obj = object_5.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(object_5base_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
