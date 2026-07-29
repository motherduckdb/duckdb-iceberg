
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/term.hpp"

namespace duckdb {
namespace rest_api_objects {

class UnaryExpression {
public:
	UnaryExpression();
	UnaryExpression(const UnaryExpression &) = delete;
	UnaryExpression &operator=(const UnaryExpression &) = delete;
	UnaryExpression(UnaryExpression &&) = default;
	UnaryExpression &operator=(UnaryExpression &&) = default;

public:
	// Deserialization
	static UnaryExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UnaryExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	Term term;
};

} // namespace rest_api_objects
} // namespace duckdb
