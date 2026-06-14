
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

class AssignUUIDUpdate {
public:
	AssignUUIDUpdate();
	AssignUUIDUpdate(const AssignUUIDUpdate &) = delete;
	AssignUUIDUpdate &operator=(const AssignUUIDUpdate &) = delete;
	AssignUUIDUpdate(AssignUUIDUpdate &&) = default;
	AssignUUIDUpdate &operator=(AssignUUIDUpdate &&) = default;

public:
	// Deserialization
	static AssignUUIDUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	AssignUUIDUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
