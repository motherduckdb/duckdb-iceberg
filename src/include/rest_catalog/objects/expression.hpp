
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/and_or_expression.hpp"
#include "rest_catalog/objects/false_expression.hpp"
#include "rest_catalog/objects/literal_expression.hpp"
#include "rest_catalog/objects/not_expression.hpp"
#include "rest_catalog/objects/set_expression.hpp"
#include "rest_catalog/objects/true_expression.hpp"
#include "rest_catalog/objects/unary_expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression {
public:
	Expression();
	Expression(const Expression &) = delete;
	Expression &operator=(const Expression &) = delete;
	Expression(Expression &&) = default;
	Expression &operator=(Expression &&) = default;

public:
	// Deserialization
	static Expression FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	Expression Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	TrueExpression true_expression;
	bool has_true_expression = false;
	FalseExpression false_expression;
	bool has_false_expression = false;
	AndOrExpression and_or_expression;
	bool has_and_or_expression = false;
	NotExpression not_expression;
	bool has_not_expression = false;
	SetExpression set_expression;
	bool has_set_expression = false;
	LiteralExpression literal_expression;
	bool has_literal_expression = false;
	UnaryExpression unary_expression;
	bool has_unary_expression = false;
};

} // namespace rest_api_objects
} // namespace duckdb
