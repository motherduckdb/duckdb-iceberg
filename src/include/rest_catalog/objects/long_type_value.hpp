
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LongTypeValue {
public:
	LongTypeValue();
	LongTypeValue(const LongTypeValue &) = delete;
	LongTypeValue &operator=(const LongTypeValue &) = delete;
	LongTypeValue(LongTypeValue &&) = default;
	LongTypeValue &operator=(LongTypeValue &&) = default;

public:
	// Deserialization
	static LongTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	LongTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
