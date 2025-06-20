
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoveSchemasUpdate {
public:
	RemoveSchemasUpdate();
	RemoveSchemasUpdate(const RemoveSchemasUpdate &) = delete;
	RemoveSchemasUpdate &operator=(const RemoveSchemasUpdate &) = delete;
	RemoveSchemasUpdate(RemoveSchemasUpdate &&) = default;
	RemoveSchemasUpdate &operator=(RemoveSchemasUpdate &&) = default;

public:
	static RemoveSchemasUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	vector<int32_t> schema_ids;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
