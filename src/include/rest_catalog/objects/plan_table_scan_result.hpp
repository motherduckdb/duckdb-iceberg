
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/async_planning_result.hpp"
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanResult {
public:
	PlanTableScanResult();
	PlanTableScanResult(const PlanTableScanResult &) = delete;
	PlanTableScanResult &operator=(const PlanTableScanResult &) = delete;
	PlanTableScanResult(PlanTableScanResult &&) = default;
	PlanTableScanResult &operator=(PlanTableScanResult &&) = default;

public:
	// Deserialization
	static PlanTableScanResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PlanTableScanResult Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	CompletedPlanningWithIDResult completed_planning_with_idresult;
	bool has_completed_planning_with_idresult = false;
	FailedPlanningResult failed_planning_result;
	bool has_failed_planning_result = false;
	AsyncPlanningResult async_planning_result;
	bool has_async_planning_result = false;
	EmptyPlanningResult empty_planning_result;
	bool has_empty_planning_result = false;
};

} // namespace rest_api_objects
} // namespace duckdb
