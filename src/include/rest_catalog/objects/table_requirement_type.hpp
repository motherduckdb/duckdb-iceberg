
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableRequirementType {
public:
	TableRequirementType();
	TableRequirementType(const TableRequirementType &) = delete;
	TableRequirementType &operator=(const TableRequirementType &) = delete;
	TableRequirementType(TableRequirementType &&) = default;
	TableRequirementType &operator=(TableRequirementType &&) = default;

public:
	// Deserialization
	static TableRequirementType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	TableRequirementType Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
