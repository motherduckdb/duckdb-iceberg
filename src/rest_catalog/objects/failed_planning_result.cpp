
#include "rest_catalog/objects/failed_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FailedPlanningResult::FailedPlanningResult() {
}
FailedPlanningResult::Object7::Object7() {
}

FailedPlanningResult::Object7 FailedPlanningResult::Object7::FromJSON(JSONValue obj) {
	Object7 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FailedPlanningResult::Object7 FailedPlanningResult::Object7::Copy() const {
	Object7 res;
	res.status = status.Copy();
	return res;
}

string FailedPlanningResult::Object7::TryFromJSON(JSONValue obj) {
	string error;
	auto status_val = obj.GetMember("status");
	if (!status_val.IsValid()) {
		return "Object7 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FailedPlanningResult::Object7::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: status
	auto status_json = status.ToJSON(writer);
	obj.Add("status", status_json);
}

JSONMutableValue FailedPlanningResult::Object7::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

FailedPlanningResult FailedPlanningResult::FromJSON(JSONValue obj) {
	FailedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FailedPlanningResult FailedPlanningResult::Copy() const {
	FailedPlanningResult res;
	res.iceberg_error_response = iceberg_error_response.Copy();
	res.object_7 = object_7.Copy();
	return res;
}

string FailedPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	error = iceberg_error_response.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_7.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void FailedPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: IcebergErrorResponse
	iceberg_error_response.PopulateJSON(writer, obj);

	// Serialize base class: Object7
	object_7.PopulateJSON(writer, obj);
}

JSONMutableValue FailedPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
