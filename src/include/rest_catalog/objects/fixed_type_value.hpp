
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FixedTypeValue {
public:
	FixedTypeValue();
	FixedTypeValue(const FixedTypeValue &) = delete;
	FixedTypeValue &operator=(const FixedTypeValue &) = delete;
	FixedTypeValue(FixedTypeValue &&) = default;
	FixedTypeValue &operator=(FixedTypeValue &&) = default;

public:
	// Deserialization
	static FixedTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FixedTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
