
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FloatTypeValue {
public:
	FloatTypeValue();
	FloatTypeValue(const FloatTypeValue &) = delete;
	FloatTypeValue &operator=(const FloatTypeValue &) = delete;
	FloatTypeValue(FloatTypeValue &&) = default;
	FloatTypeValue &operator=(FloatTypeValue &&) = default;

public:
	// Deserialization
	static FloatTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FloatTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
