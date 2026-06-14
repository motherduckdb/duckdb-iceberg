
#include "rest_catalog/objects/not_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

NotExpression::NotExpression() {
}

NotExpression NotExpression::FromJSON(yyjson_val *obj) {
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

string NotExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "NotExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto child_val = yyjson_obj_get(obj, "child");
	if (!child_val) {
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

yyjson_mut_val *NotExpression::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: type
	yyjson_mut_val *type_val = type.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);

	// Serialize: child
	yyjson_mut_val *child_val = child->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "child", child_val);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
