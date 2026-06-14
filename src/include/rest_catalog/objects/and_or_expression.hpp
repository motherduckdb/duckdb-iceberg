
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

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
	static AndOrExpression FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	AndOrExpression Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	ExpressionType type;
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};

} // namespace rest_api_objects
} // namespace duckdb
