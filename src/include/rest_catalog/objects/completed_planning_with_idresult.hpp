
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningWithIDResult {
public:
	CompletedPlanningWithIDResult();
	CompletedPlanningWithIDResult(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult &operator=(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult(CompletedPlanningWithIDResult &&) = default;
	CompletedPlanningWithIDResult &operator=(CompletedPlanningWithIDResult &&) = default;
	class Object6 {
	public:
		Object6();
		Object6(const Object6 &) = delete;
		Object6 &operator=(const Object6 &) = delete;
		Object6(Object6 &&) = default;
		Object6 &operator=(Object6 &&) = default;

	public:
		// Deserialization
		static Object6 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object6 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string plan_id;
	};

public:
	// Deserialization
	static CompletedPlanningWithIDResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CompletedPlanningWithIDResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	CompletedPlanningResult completed_planning_result;
	Object6 object_6;
};

} // namespace rest_api_objects
} // namespace duckdb
