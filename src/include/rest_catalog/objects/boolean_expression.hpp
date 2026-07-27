
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class BooleanExpression {
public:
	BooleanExpression();
	BooleanExpression(const BooleanExpression &) = delete;
	BooleanExpression &operator=(const BooleanExpression &) = delete;
	BooleanExpression(BooleanExpression &&) = default;
	BooleanExpression &operator=(BooleanExpression &&) = default;

public:
	// Deserialization
	static BooleanExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	BooleanExpression Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	bool value;
};

} // namespace rest_api_objects
} // namespace duckdb
