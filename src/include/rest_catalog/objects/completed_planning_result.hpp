
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/plan_status.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningResult {
public:
	CompletedPlanningResult();
	CompletedPlanningResult(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult &operator=(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult(CompletedPlanningResult &&) = default;
	CompletedPlanningResult &operator=(CompletedPlanningResult &&) = default;
	class Object5 {
	public:
		Object5();
		Object5(const Object5 &) = delete;
		Object5 &operator=(const Object5 &) = delete;
		Object5(Object5 &&) = default;
		Object5 &operator=(Object5 &&) = default;

	public:
		// Deserialization
		static Object5 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object5 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		PlanStatus status;
		optional<vector<StorageCredential>> storage_credentials;
	};

public:
	// Deserialization
	static CompletedPlanningResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CompletedPlanningResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ScanTasks scan_tasks;
	Object5 object_5;
};

} // namespace rest_api_objects
} // namespace duckdb
