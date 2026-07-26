
#include "rest_catalog/objects/function_definition.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDefinition::FunctionDefinition() {
}

FunctionDefinition FunctionDefinition::FromJSON(JSONValue obj) {
	FunctionDefinition res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinition FunctionDefinition::Copy() const {
	FunctionDefinition res;
	res.definition_id = definition_id;
	res.parameters.reserve(parameters.size());
	for (auto &item : parameters) {
		res.parameters.emplace_back(item.Copy());
	}
	res.return_type = return_type ? make_uniq<FunctionDataType>(return_type->Copy()) : nullptr;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.current_version_id = current_version_id;
	res.function_type = function_type;
	if (return_nullable.has_value()) {
		res.return_nullable.emplace();
		(*res.return_nullable) = (*return_nullable);
	}
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionDefinition::TryFromJSON(JSONValue obj) {
	string error;
	auto definition_id_val = obj.GetMember("definition-id");
	if (!definition_id_val.IsValid()) {
		return "FunctionDefinition required property 'definition-id' is missing";
	} else {
		if (json_utils::IsString(definition_id_val)) {
			definition_id = json_utils::GetString(definition_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'definition_id' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(definition_id_val).c_str());
		}
	}
	auto parameters_val = obj.GetMember("parameters");
	if (!parameters_val.IsValid()) {
		return "FunctionDefinition required property 'parameters' is missing";
	} else {
		if (parameters_val.IsArray()) {
			parameters_val.IterateArray([&](JSONValue parameters_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionParameter parameters_item;
				error = parameters_item.TryFromJSON(parameters_item_val);
				if (!error.empty()) {
					return;
				}
				parameters.emplace_back(std::move(parameters_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'parameters' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(parameters_val).c_str());
		}
	}
	auto return_type_val = obj.GetMember("return-type");
	if (!return_type_val.IsValid()) {
		return "FunctionDefinition required property 'return-type' is missing";
	} else {
		return_type = make_uniq<FunctionDataType>();
		error = return_type->TryFromJSON(return_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto versions_val = obj.GetMember("versions");
	if (!versions_val.IsValid()) {
		return "FunctionDefinition required property 'versions' is missing";
	} else {
		if (versions_val.IsArray()) {
			versions_val.IterateArray([&](JSONValue versions_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinitionVersion versions_item;
				error = versions_item.TryFromJSON(versions_item_val);
				if (!error.empty()) {
					return;
				}
				versions.emplace_back(std::move(versions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("FunctionDefinition property 'versions' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(versions_val).c_str());
		}
	}
	auto current_version_id_val = obj.GetMember("current-version-id");
	if (!current_version_id_val.IsValid()) {
		return "FunctionDefinition required property 'current-version-id' is missing";
	} else {
		if (json_utils::IsInteger(current_version_id_val)) {
			current_version_id = json_utils::GetSignedInteger(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'current_version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_version_id_val).c_str());
		}
	}
	auto function_type_val = obj.GetMember("function-type");
	if (!function_type_val.IsValid()) {
		return "FunctionDefinition required property 'function-type' is missing";
	} else {
		if (json_utils::IsString(function_type_val)) {
			function_type = json_utils::GetString(function_type_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'function_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(function_type_val).c_str());
		}
	}
	auto return_nullable_val = obj.GetMember("return-nullable");
	if (return_nullable_val.IsValid()) {
		bool return_nullable_tmp;
		if (json_utils::IsBoolean(return_nullable_val)) {
			return_nullable_tmp = json_utils::GetBoolean(return_nullable_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'return_nullable_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(return_nullable_val).c_str());
		}
		return_nullable = std::move(return_nullable_tmp);
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property '_doc_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionDefinition::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: definition-id
	obj.AddString("definition-id", definition_id);

	// Serialize: parameters
	auto parameters_arr = writer.CreateArray();
	for (const auto &item : parameters) {
		auto item_val = item.ToJSON(writer);
		parameters_arr.Append(item_val);
	}
	obj.Add("parameters", parameters_arr);

	// Serialize: return-type
	auto return_type_val = return_type->ToJSON(writer);
	obj.Add("return-type", return_type_val);

	// Serialize: versions
	auto versions_arr = writer.CreateArray();
	for (const auto &item : versions) {
		auto item_val = item.ToJSON(writer);
		versions_arr.Append(item_val);
	}
	obj.Add("versions", versions_arr);

	// Serialize: current-version-id
	obj.Add("current-version-id", writer.CreateSignedInteger(current_version_id));

	// Serialize: function-type
	obj.AddString("function-type", function_type);

	// Serialize: return-nullable
	if (return_nullable.has_value()) {
		auto &return_nullable_value = *return_nullable;
		obj.Add("return-nullable", writer.CreateBoolean(return_nullable_value));
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		obj.AddString("doc", _doc_value);
	}
}

JSONMutableValue FunctionDefinition::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
