
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetStatisticsUpdate {
public:
	SetStatisticsUpdate();
	SetStatisticsUpdate(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate &operator=(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate(SetStatisticsUpdate &&) = default;
	SetStatisticsUpdate &operator=(SetStatisticsUpdate &&) = default;

public:
	// Deserialization
	static SetStatisticsUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	SetStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	StatisticsFile statistics;
	int64_t snapshot_id;
	bool has_snapshot_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
