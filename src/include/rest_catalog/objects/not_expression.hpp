
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

class NotExpression {
public:
	NotExpression();
	NotExpression(const NotExpression &) = delete;
	NotExpression &operator=(const NotExpression &) = delete;
	NotExpression(NotExpression &&) = default;
	NotExpression &operator=(NotExpression &&) = default;

public:
	// Deserialization
	static NotExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	NotExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	unique_ptr<Expression> child;
};

} // namespace rest_api_objects
} // namespace duckdb
