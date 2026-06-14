
#include "rest_catalog/objects/fetch_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchPlanningResult::FetchPlanningResult() {
}

FetchPlanningResult FetchPlanningResult::FromJSON(yyjson_val *obj) {
	FetchPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FetchPlanningResult FetchPlanningResult::Copy() const {
	FetchPlanningResult res;
	if (has_completed_planning_result) {
		res.completed_planning_result = completed_planning_result.Copy();
	}
	res.has_completed_planning_result = has_completed_planning_result;
	if (has_failed_planning_result) {
		res.failed_planning_result = failed_planning_result.Copy();
	}
	res.has_failed_planning_result = has_failed_planning_result;
	if (has_empty_planning_result) {
		res.empty_planning_result = empty_planning_result.Copy();
	}
	res.has_empty_planning_result = has_empty_planning_result;
	return res;
}

string FetchPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = completed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_completed_planning_result = true;
			break;
		}
		error = failed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_failed_planning_result = true;
			break;
		}
		error = empty_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_empty_planning_result = true;
			break;
		}
		return "FetchPlanningResult failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void FetchPlanningResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (has_completed_planning_result) {
		completed_planning_result.PopulateJSON(doc, obj);
	} else if (has_failed_planning_result) {
		failed_planning_result.PopulateJSON(doc, obj);
	} else if (has_empty_planning_result) {
		empty_planning_result.PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *FetchPlanningResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
