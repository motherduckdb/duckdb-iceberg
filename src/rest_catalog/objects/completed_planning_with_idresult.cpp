
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Object6 Object6::FromJSON(yyjson_val *obj) {
	Object6 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Object6::TryFromJSON(yyjson_val *obj) {
	string error;
	auto plan_id_val = yyjson_obj_get(obj, "plan-id");
	if (plan_id_val) {
		has_plan_id = true;
		if (yyjson_is_str(plan_id_val)) {
			plan_id = yyjson_get_str(plan_id_val);
		} else {
			return StringUtil::Format("Object6 property 'plan_id' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(plan_id_val));
		}
	}
	return "";
}

yyjson_mut_val *Object6::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: plan-id
	if (has_plan_id) {
		yyjson_mut_obj_add_str(doc, obj, "plan-id", plan_id.c_str());
	}

	return obj;
}

CompletedPlanningWithIDResult CompletedPlanningWithIDResult::FromJSON(yyjson_val *obj) {
	CompletedPlanningWithIDResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CompletedPlanningWithIDResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = completed_planning_result.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_6.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

yyjson_mut_val *CompletedPlanningWithIDResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: CompletedPlanningResult
	yyjson_mut_val *completed_planning_resultbase_obj = completed_planning_result.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(completed_planning_resultbase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize base class: Object6
	yyjson_mut_val *object_6base_obj = object_6.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(object_6base_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
