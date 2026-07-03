
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructField;

class StructType {
public:
	StructType();
	StructType(const StructType &) = delete;
	StructType &operator=(const StructType &) = delete;
	StructType(StructType &&) = default;
	StructType &operator=(StructType &&) = default;

public:
	// Deserialization
	static StructType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	StructType Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	vector<unique_ptr<StructField>> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
