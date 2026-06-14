
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionSpecsUpdate {
public:
	RemovePartitionSpecsUpdate();
	RemovePartitionSpecsUpdate(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate &operator=(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate(RemovePartitionSpecsUpdate &&) = default;
	RemovePartitionSpecsUpdate &operator=(RemovePartitionSpecsUpdate &&) = default;

public:
	// Deserialization
	static RemovePartitionSpecsUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemovePartitionSpecsUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	vector<int32_t> spec_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
