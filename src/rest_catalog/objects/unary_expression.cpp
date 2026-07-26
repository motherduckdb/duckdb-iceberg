
#include "rest_catalog/objects/unary_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

UnaryExpression::UnaryExpression() {
}

UnaryExpression UnaryExpression::FromJSON(JSONValue obj) {
	UnaryExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UnaryExpression UnaryExpression::Copy() const {
	UnaryExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
	return res;
}

string UnaryExpression::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "UnaryExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = obj.GetMember("term");
	if (!term_val.IsValid()) {
		return "UnaryExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void UnaryExpression::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_val = type.ToJSON(writer);
	obj.Add("type", type_val);

	// Serialize: term
	auto term_val = term.ToJSON(writer);
	obj.Add("term", term_val);
}

JSONMutableValue UnaryExpression::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
