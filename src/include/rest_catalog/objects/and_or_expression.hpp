
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"

namespace duckdb {
namespace rest_api_objects {

class Expression;

class AndOrExpression {
public:
	AndOrExpression();
	AndOrExpression(const AndOrExpression &) = delete;
	AndOrExpression &operator=(const AndOrExpression &) = delete;
	AndOrExpression(AndOrExpression &&) = default;
	AndOrExpression &operator=(AndOrExpression &&) = default;

public:
	// Deserialization
	static AndOrExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AndOrExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};

} // namespace rest_api_objects
} // namespace duckdb
