
#include "rest_catalog/objects/function_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionMetadata::FunctionMetadata() {
}

FunctionMetadata FunctionMetadata::FromJSON(JSONValue obj) {
	FunctionMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionMetadata FunctionMetadata::Copy() const {
	FunctionMetadata res;
	res.function_uuid = function_uuid;
	res.format_version = format_version;
	res.definitions.reserve(definitions.size());
	for (auto &item : definitions) {
		res.definitions.emplace_back(item.Copy());
	}
	res.definition_log.reserve(definition_log.size());
	for (auto &item : definition_log) {
		res.definition_log.emplace_back(item.Copy());
	}
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	if (secure.has_value()) {
		res.secure.emplace();
		(*res.secure) = (*secure);
	}
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto function_uuid_val = obj.GetMember("function-uuid");
	if (!function_uuid_val.IsValid()) {
		return "FunctionMetadata required property 'function-uuid' is missing";
	} else {
		if (json_utils::IsString(function_uuid_val)) {
			function_uuid = json_utils::GetString(function_uuid_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'function_uuid' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(function_uuid_val).c_str());
		}
	}
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "FunctionMetadata required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	auto definitions_val = obj.GetMember("definitions");
	if (!definitions_val.IsValid()) {
		return "FunctionMetadata required property 'definitions' is missing";
	} else {
		if (definitions_val.IsArray()) {
			definitions_val.IterateArray([&](JSONValue definitions_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinition definitions_item;
				error = definitions_item.TryFromJSON(definitions_item_val);
				if (!error.empty()) {
					return;
				}
				definitions.emplace_back(std::move(definitions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definitions' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(definitions_val).c_str());
		}
	}
	auto definition_log_val = obj.GetMember("definition-log");
	if (!definition_log_val.IsValid()) {
		return "FunctionMetadata required property 'definition-log' is missing";
	} else {
		if (definition_log_val.IsArray()) {
			definition_log_val.IterateArray([&](JSONValue definition_log_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinitionLogEntry definition_log_item;
				error = definition_log_item.TryFromJSON(definition_log_item_val);
				if (!error.empty()) {
					return;
				}
				definition_log.emplace_back(std::move(definition_log_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definition_log' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(definition_log_val).c_str());
		}
	}
	auto location_val = obj.GetMember("location");
	if (location_val.IsValid()) {
		string location_tmp;
		if (json_utils::IsString(location_val)) {
			location_tmp = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'location_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(location_val).c_str());
		}
		location = std::move(location_tmp);
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		case_insensitive_map_t<string> properties_tmp;
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error =
					    StringUtil::Format("FunctionMetadata property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "FunctionMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto secure_val = obj.GetMember("secure");
	if (secure_val.IsValid()) {
		bool secure_tmp;
		if (json_utils::IsBoolean(secure_val)) {
			secure_tmp = json_utils::GetBoolean(secure_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'secure_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(secure_val).c_str());
		}
		secure = std::move(secure_tmp);
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format("FunctionMetadata property '_doc_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: function-uuid
	obj.AddString("function-uuid", function_uuid);

	// Serialize: format-version
	obj.Add("format-version", writer.CreateSignedInteger(format_version));

	// Serialize: definitions
	auto definitions_arr = writer.CreateArray();
	for (const auto &item : definitions) {
		auto item_val = item.ToJSON(writer);
		definitions_arr.Append(item_val);
	}
	obj.Add("definitions", definitions_arr);

	// Serialize: definition-log
	auto definition_log_arr = writer.CreateArray();
	for (const auto &item : definition_log) {
		auto item_val = item.ToJSON(writer);
		definition_log_arr.Append(item_val);
	}
	obj.Add("definition-log", definition_log_arr);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		obj.AddString("location", location_value);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		auto properties_value_obj = writer.CreateObject();
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			properties_value_obj.AddString(key, value);
		}
		obj.Add("properties", properties_value_obj);
	}

	// Serialize: secure
	if (secure.has_value()) {
		auto &secure_value = *secure;
		obj.Add("secure", writer.CreateBoolean(secure_value));
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		obj.AddString("doc", _doc_value);
	}
}

JSONMutableValue FunctionMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
