
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/field_name.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class PlanTableScanRequest {
public:
	PlanTableScanRequest();
	PlanTableScanRequest(const PlanTableScanRequest &) = delete;
	PlanTableScanRequest &operator=(const PlanTableScanRequest &) = delete;
	PlanTableScanRequest(PlanTableScanRequest &&) = default;
	PlanTableScanRequest &operator=(PlanTableScanRequest &&) = default;

public:
	// Deserialization
	static PlanTableScanRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PlanTableScanRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<int64_t> snapshot_id;
	optional<vector<FieldName>> select;
	unique_ptr<Expression> filter;
	optional<int64_t> min_rows_requested;
	optional<bool> case_sensitive;
	optional<bool> use_snapshot_schema;
	optional<int64_t> start_snapshot_id;
	optional<int64_t> end_snapshot_id;
	optional<vector<FieldName>> stats_fields;
};

} // namespace rest_api_objects
} // namespace duckdb
