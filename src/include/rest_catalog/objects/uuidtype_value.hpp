
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UUIDTypeValue {
public:
	UUIDTypeValue();
	UUIDTypeValue(const UUIDTypeValue &) = delete;
	UUIDTypeValue &operator=(const UUIDTypeValue &) = delete;
	UUIDTypeValue(UUIDTypeValue &&) = default;
	UUIDTypeValue &operator=(UUIDTypeValue &&) = default;

public:
	// Deserialization
	static UUIDTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	UUIDTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
