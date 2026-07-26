
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CompletedPlanningWithIDResult::CompletedPlanningWithIDResult() {
}
CompletedPlanningWithIDResult::Object6::Object6() {
}

CompletedPlanningWithIDResult::Object6 CompletedPlanningWithIDResult::Object6::FromJSON(JSONValue obj) {
	Object6 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningWithIDResult::Object6 CompletedPlanningWithIDResult::Object6::Copy() const {
	Object6 res;
	res.plan_id = plan_id;
	return res;
}

string CompletedPlanningWithIDResult::Object6::TryFromJSON(JSONValue obj) {
	string error;
	auto plan_id_val = obj.GetMember("plan-id");
	if (!plan_id_val.IsValid()) {
		return "Object6 required property 'plan-id' is missing";
	} else {
		if (json_utils::IsString(plan_id_val)) {
			plan_id = json_utils::GetString(plan_id_val);
		} else {
			return StringUtil::Format("Object6 property 'plan_id' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(plan_id_val).c_str());
		}
	}
	return "";
}

void CompletedPlanningWithIDResult::Object6::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: plan-id
	auto plan_id_json = writer.CreateString(plan_id);
	obj.Add("plan-id", plan_id_json);
}

JSONMutableValue CompletedPlanningWithIDResult::Object6::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

CompletedPlanningWithIDResult CompletedPlanningWithIDResult::FromJSON(JSONValue obj) {
	CompletedPlanningWithIDResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningWithIDResult CompletedPlanningWithIDResult::Copy() const {
	CompletedPlanningWithIDResult res;
	res.completed_planning_result = completed_planning_result.Copy();
	res.object_6 = object_6.Copy();
	return res;
}

string CompletedPlanningWithIDResult::TryFromJSON(JSONValue obj) {
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

void CompletedPlanningWithIDResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: CompletedPlanningResult
	completed_planning_result.PopulateJSON(writer, obj);

	// Serialize base class: Object6
	object_6.PopulateJSON(writer, obj);
}

JSONMutableValue CompletedPlanningWithIDResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
