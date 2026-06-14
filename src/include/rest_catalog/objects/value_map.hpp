
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ValueMap {
public:
	ValueMap();
	ValueMap(const ValueMap &) = delete;
	ValueMap &operator=(const ValueMap &) = delete;
	ValueMap(ValueMap &&) = default;
	ValueMap &operator=(ValueMap &&) = default;

public:
	// Deserialization
	static ValueMap FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ValueMap Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<vector<IntegerTypeValue>> keys;
	optional<vector<PrimitiveTypeValue>> values;
};

} // namespace rest_api_objects
} // namespace duckdb
