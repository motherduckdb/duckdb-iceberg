
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Object7 {
public:
	// Deserialization
	static Object7 FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	PlanStatus status;
};

class FailedPlanningResult {
public:
	// Deserialization
	static FailedPlanningResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	IcebergErrorResponse iceberg_error_response;
	Object7 object_7;
};

} // namespace rest_api_objects
} // namespace duckdb
