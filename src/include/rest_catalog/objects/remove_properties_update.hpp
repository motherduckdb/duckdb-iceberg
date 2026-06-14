
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePropertiesUpdate {
public:
	RemovePropertiesUpdate();
	RemovePropertiesUpdate(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate &operator=(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate(RemovePropertiesUpdate &&) = default;
	RemovePropertiesUpdate &operator=(RemovePropertiesUpdate &&) = default;

public:
	// Deserialization
	static RemovePropertiesUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemovePropertiesUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	vector<string> removals;
};

} // namespace rest_api_objects
} // namespace duckdb
