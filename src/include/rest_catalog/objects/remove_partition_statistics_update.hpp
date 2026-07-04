
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionStatisticsUpdate {
public:
	RemovePartitionStatisticsUpdate();
	RemovePartitionStatisticsUpdate(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate &operator=(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate(RemovePartitionStatisticsUpdate &&) = default;
	RemovePartitionStatisticsUpdate &operator=(RemovePartitionStatisticsUpdate &&) = default;

public:
	// Deserialization
	static RemovePartitionStatisticsUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemovePartitionStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	int64_t snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
