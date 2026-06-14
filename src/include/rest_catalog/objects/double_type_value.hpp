
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DoubleTypeValue {
public:
	DoubleTypeValue();
	DoubleTypeValue(const DoubleTypeValue &) = delete;
	DoubleTypeValue &operator=(const DoubleTypeValue &) = delete;
	DoubleTypeValue(DoubleTypeValue &&) = default;
	DoubleTypeValue &operator=(DoubleTypeValue &&) = default;

public:
	// Deserialization
	static DoubleTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	DoubleTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
