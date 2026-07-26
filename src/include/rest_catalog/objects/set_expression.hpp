
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

class SetExpression {
public:
	SetExpression();
	SetExpression(const SetExpression &) = delete;
	SetExpression &operator=(const SetExpression &) = delete;
	SetExpression(SetExpression &&) = default;
	SetExpression &operator=(SetExpression &&) = default;

public:
	// Deserialization
	static SetExpression FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetExpression Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ExpressionType type;
	Term term;
	vector<PrimitiveTypeValue> values;
};

} // namespace rest_api_objects
} // namespace duckdb
