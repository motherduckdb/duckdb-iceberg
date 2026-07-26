
#include "rest_catalog/objects/literal_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LiteralExpression::LiteralExpression() {
}

LiteralExpression LiteralExpression::FromJSON(JSONValue obj) {
	LiteralExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LiteralExpression LiteralExpression::Copy() const {
	LiteralExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
	res.value = value.Copy();
	return res;
}

string LiteralExpression::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "LiteralExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = obj.GetMember("term");
	if (!term_val.IsValid()) {
		return "LiteralExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_val = obj.GetMember("value");
	if (!value_val.IsValid()) {
		return "LiteralExpression required property 'value' is missing";
	} else {
		error = value.TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void LiteralExpression::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = type.ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: term
	auto term_json = term.ToJSON(writer);
	obj.Add("term", term_json);

	// Serialize: value
	auto value_json = value.ToJSON(writer);
	obj.Add("value", value_json);
}

JSONMutableValue LiteralExpression::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
