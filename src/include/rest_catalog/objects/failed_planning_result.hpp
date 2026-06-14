
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

class FailedPlanningResult {
public:
	FailedPlanningResult();
	FailedPlanningResult(const FailedPlanningResult &) = delete;
	FailedPlanningResult &operator=(const FailedPlanningResult &) = delete;
	FailedPlanningResult(FailedPlanningResult &&) = default;
	FailedPlanningResult &operator=(FailedPlanningResult &&) = default;
	class Object7 {
	public:
		Object7();
		Object7(const Object7 &) = delete;
		Object7 &operator=(const Object7 &) = delete;
		Object7(Object7 &&) = default;
		Object7 &operator=(Object7 &&) = default;

	public:
		// Deserialization
		static Object7 FromJSON(yyjson_val *obj);
		string TryFromJSON(yyjson_val *obj);

		// Copy
		Object7 Copy() const;

		// Serialization
		void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
		yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	public:
		PlanStatus status;
	};

public:
	// Deserialization
	static FailedPlanningResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FailedPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	IcebergErrorResponse iceberg_error_response;
	Object7 object_7;
};

} // namespace rest_api_objects
} // namespace duckdb
