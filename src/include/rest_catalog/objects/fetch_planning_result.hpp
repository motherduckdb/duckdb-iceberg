
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchPlanningResult {
public:
	FetchPlanningResult();
	FetchPlanningResult(const FetchPlanningResult &) = delete;
	FetchPlanningResult &operator=(const FetchPlanningResult &) = delete;
	FetchPlanningResult(FetchPlanningResult &&) = default;
	FetchPlanningResult &operator=(FetchPlanningResult &&) = default;

public:
	// Deserialization
	static FetchPlanningResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FetchPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<CompletedPlanningResult> completed_planning_result;
	optional<FailedPlanningResult> failed_planning_result;
	optional<EmptyPlanningResult> empty_planning_result;
};

} // namespace rest_api_objects
} // namespace duckdb
