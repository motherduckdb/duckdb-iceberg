
#include "rest_catalog/objects/plan_table_scan_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PlanTableScanResult PlanTableScanResult::FromJSON(yyjson_val *obj) {
	PlanTableScanResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PlanTableScanResult::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = completed_planning_with_idresult.TryFromJSON(obj);
		if (error.empty()) {
			has_completed_planning_with_idresult = true;
			break;
		}
		error = failed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_failed_planning_result = true;
			break;
		}
		error = async_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_async_planning_result = true;
			break;
		}
		error = empty_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_empty_planning_result = true;
			break;
		}
		return "PlanTableScanResult failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *PlanTableScanResult::ToJSON(yyjson_mut_doc *doc) const {
	if (has_completed_planning_with_idresult) {
		return completed_planning_with_idresult.ToJSON(doc);
	} else if (has_failed_planning_result) {
		return failed_planning_result.ToJSON(doc);
	} else if (has_async_planning_result) {
		return async_planning_result.ToJSON(doc);
	} else if (has_empty_planning_result) {
		return empty_planning_result.ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
