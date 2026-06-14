
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionField {
public:
	PartitionField();
	PartitionField(const PartitionField &) = delete;
	PartitionField &operator=(const PartitionField &) = delete;
	PartitionField(PartitionField &&) = default;
	PartitionField &operator=(PartitionField &&) = default;

public:
	// Deserialization
	static PartitionField FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PartitionField Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t source_id;
	Transform transform;
	string name;
	int32_t field_id;
	bool has_field_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
