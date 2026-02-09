
#include "rest_catalog/objects/expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Expression Expression::FromJSON(yyjson_val *obj) {
	Expression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Expression::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = true_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_true_expression = true;
			break;
		}
		error = false_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_false_expression = true;
			break;
		}
		error = and_or_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_and_or_expression = true;
			break;
		}
		error = not_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_not_expression = true;
			break;
		}
		error = set_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_set_expression = true;
			break;
		}
		error = literal_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_literal_expression = true;
			break;
		}
		error = unary_expression.TryFromJSON(obj);
		if (error.empty()) {
			has_unary_expression = true;
			break;
		}
		return "Expression failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *Expression::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	if (has_true_expression) {
		return true_expression.ToJSON(doc);
	} else if (has_false_expression) {
		return false_expression.ToJSON(doc);
	} else if (has_and_or_expression) {
		return and_or_expression.ToJSON(doc);
	} else if (has_not_expression) {
		return not_expression.ToJSON(doc);
	} else if (has_set_expression) {
		return set_expression.ToJSON(doc);
	} else if (has_literal_expression) {
		return literal_expression.ToJSON(doc);
	} else if (has_unary_expression) {
		return unary_expression.ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
