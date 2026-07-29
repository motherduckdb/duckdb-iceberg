
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"
#include "rest_catalog/objects/term.hpp"

namespace duckdb {
namespace rest_api_objects {

class LiteralExpression {
public:
	LiteralExpression();
	LiteralExpression(const LiteralExpression &) = delete;
	LiteralExpression &operator=(const LiteralExpression &) = delete;
	LiteralExpression(LiteralExpression &&) = default;
	LiteralExpression &operator=(LiteralExpression &&) = default;

public:
	// Deserialization
	static LiteralExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LiteralExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	Term term;
	PrimitiveTypeValue value;
};

} // namespace rest_api_objects
} // namespace duckdb
