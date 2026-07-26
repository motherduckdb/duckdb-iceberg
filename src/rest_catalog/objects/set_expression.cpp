
#include "rest_catalog/objects/set_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetExpression::SetExpression() {
}

SetExpression SetExpression::FromJSON(JSONValue obj) {
	SetExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetExpression SetExpression::Copy() const {
	SetExpression res;
	res.type = type.Copy();
	res.term = term.Copy();
	res.values.reserve(values.size());
	for (auto &item : values) {
		res.values.emplace_back(item.Copy());
	}
	return res;
}

string SetExpression::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "SetExpression required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto term_val = obj.GetMember("term");
	if (!term_val.IsValid()) {
		return "SetExpression required property 'term' is missing";
	} else {
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto values_val = obj.GetMember("values");
	if (!values_val.IsValid()) {
		return "SetExpression required property 'values' is missing";
	} else {
		if (values_val.IsArray()) {
			values_val.IterateArray([&](JSONValue values_item_val) {
				if (!error.empty()) {
					return;
				}
				PrimitiveTypeValue values_item;
				error = values_item.TryFromJSON(values_item_val);
				if (!error.empty()) {
					return;
				}
				values.emplace_back(std::move(values_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("SetExpression property 'values' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(values_val).c_str());
		}
	}
	return "";
}

void SetExpression::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_val = type.ToJSON(writer);
	obj.Add("type", type_val);

	// Serialize: term
	auto term_val = term.ToJSON(writer);
	obj.Add("term", term_val);

	// Serialize: values
	auto values_arr = writer.CreateArray();
	for (const auto &item : values) {
		auto item_val = item.ToJSON(writer);
		values_arr.Append(item_val);
	}
	obj.Add("values", values_arr);
}

JSONMutableValue SetExpression::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
