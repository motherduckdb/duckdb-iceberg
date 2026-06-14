
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/sort_order.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateTableRequest {
public:
	CreateTableRequest();
	CreateTableRequest(const CreateTableRequest &) = delete;
	CreateTableRequest &operator=(const CreateTableRequest &) = delete;
	CreateTableRequest(CreateTableRequest &&) = default;
	CreateTableRequest &operator=(CreateTableRequest &&) = default;

public:
	// Deserialization
	static CreateTableRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CreateTableRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string name;
	Schema schema;
	optional<string> location;
	optional<PartitionSpec> partition_spec;
	optional<SortOrder> write_order;
	optional<bool> stage_create;
	optional<case_insensitive_map_t<string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
