
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimestampTzNanoTypeValue {
public:
	TimestampTzNanoTypeValue();
	TimestampTzNanoTypeValue(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue &operator=(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue(TimestampTzNanoTypeValue &&) = default;
	TimestampTzNanoTypeValue &operator=(TimestampTzNanoTypeValue &&) = default;

public:
	// Deserialization
	static TimestampTzNanoTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	TimestampTzNanoTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
