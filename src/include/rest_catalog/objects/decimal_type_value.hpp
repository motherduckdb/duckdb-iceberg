
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DecimalTypeValue {
public:
	DecimalTypeValue();
	DecimalTypeValue(const DecimalTypeValue &) = delete;
	DecimalTypeValue &operator=(const DecimalTypeValue &) = delete;
	DecimalTypeValue(DecimalTypeValue &&) = default;
	DecimalTypeValue &operator=(DecimalTypeValue &&) = default;

public:
	// Deserialization
	static DecimalTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	DecimalTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
