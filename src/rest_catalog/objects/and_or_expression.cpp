
#include "rest_catalog/objects/and_or_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AndOrExpression::AndOrExpression() {
}

AndOrExpression AndOrExpression::FromJSON(JSONValue obj) {
	AndOrExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AndOrExpression AndOrExpression::Copy() const {
	AndOrExpression res;
	res.type = type.Copy();
	res.left = left ? make_uniq<Expression>(left->Copy()) : nullptr;
	res.right = right ? make_uniq<Expression>(right->Copy()) : nullptr;
	return res;
}

string AndOrExpression::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AndOrExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto left_val = obj.GetMember("left");
	if (!left_val.IsValid()) {
		return "AndOrExpression required property 'left' is missing";
	} else {
		left = make_uniq<Expression>();
		error = left->TryFromJSON(left_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto right_val = obj.GetMember("right");
	if (!right_val.IsValid()) {
		return "AndOrExpression required property 'right' is missing";
	} else {
		right = make_uniq<Expression>();
		error = right->TryFromJSON(right_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AndOrExpression::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_val = type.ToJSON(writer);
	obj.Add("type", type_val);

	// Serialize: left
	auto left_val = left->ToJSON(writer);
	obj.Add("left", left_val);

	// Serialize: right
	auto right_val = right->ToJSON(writer);
	obj.Add("right", right_val);
}

JSONMutableValue AndOrExpression::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
