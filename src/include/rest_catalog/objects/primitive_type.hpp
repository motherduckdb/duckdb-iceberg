
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PrimitiveType {
public:
	PrimitiveType();
	PrimitiveType(const PrimitiveType &) = delete;
	PrimitiveType &operator=(const PrimitiveType &) = delete;
	PrimitiveType(PrimitiveType &&) = default;
	PrimitiveType &operator=(PrimitiveType &&) = default;

public:
	// Deserialization
	static PrimitiveType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PrimitiveType Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
