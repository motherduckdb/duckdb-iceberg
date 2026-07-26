
#include "rest_catalog/objects/not_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

NotExpression::NotExpression() {
}

NotExpression NotExpression::FromJSON(JSONValue obj) {
	NotExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NotExpression NotExpression::Copy() const {
	NotExpression res;
	res.type = type.Copy();
	res.child = child ? make_uniq<Expression>(child->Copy()) : nullptr;
	return res;
}

string NotExpression::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "NotExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto child_val = obj.GetMember("child");
	if (!child_val.IsValid()) {
		return "NotExpression required property 'child' is missing";
	} else {
		child = make_uniq<Expression>();
		error = child->TryFromJSON(child_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void NotExpression::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_val = type.ToJSON(writer);
	obj.Add("type", type_val);

	// Serialize: child
	auto child_val = child->ToJSON(writer);
	obj.Add("child", child_val);
}

JSONMutableValue NotExpression::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
