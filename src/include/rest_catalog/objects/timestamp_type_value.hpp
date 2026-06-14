
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimestampTypeValue {
public:
	TimestampTypeValue();
	TimestampTypeValue(const TimestampTypeValue &) = delete;
	TimestampTypeValue &operator=(const TimestampTypeValue &) = delete;
	TimestampTypeValue(TimestampTypeValue &&) = default;
	TimestampTypeValue &operator=(TimestampTypeValue &&) = default;

public:
	// Deserialization
	static TimestampTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	TimestampTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
