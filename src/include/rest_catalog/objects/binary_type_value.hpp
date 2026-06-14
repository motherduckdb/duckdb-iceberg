
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BinaryTypeValue {
public:
	BinaryTypeValue();
	BinaryTypeValue(const BinaryTypeValue &) = delete;
	BinaryTypeValue &operator=(const BinaryTypeValue &) = delete;
	BinaryTypeValue(BinaryTypeValue &&) = default;
	BinaryTypeValue &operator=(BinaryTypeValue &&) = default;

public:
	// Deserialization
	static BinaryTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	BinaryTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
