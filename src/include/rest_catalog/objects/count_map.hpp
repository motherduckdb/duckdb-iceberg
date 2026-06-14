
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CountMap {
public:
	CountMap();
	CountMap(const CountMap &) = delete;
	CountMap &operator=(const CountMap &) = delete;
	CountMap(CountMap &&) = default;
	CountMap &operator=(CountMap &&) = default;

public:
	// Deserialization
	static CountMap FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CountMap Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<IntegerTypeValue> keys;
	bool has_keys = false;
	vector<LongTypeValue> values;
	bool has_values = false;
};

} // namespace rest_api_objects
} // namespace duckdb
