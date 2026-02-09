
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Object6 {
public:
	// Deserialization
	static Object6 FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string plan_id;
	bool has_plan_id = false;
};

class CompletedPlanningWithIDResult {
public:
	// Deserialization
	static CompletedPlanningWithIDResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	CompletedPlanningResult completed_planning_result;
	Object6 object_6;
};

} // namespace rest_api_objects
} // namespace duckdb
