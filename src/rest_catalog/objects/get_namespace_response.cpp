
#include "rest_catalog/objects/get_namespace_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

GetNamespaceResponse::GetNamespaceResponse() {
}

GetNamespaceResponse GetNamespaceResponse::FromJSON(JSONValue obj) {
	GetNamespaceResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

GetNamespaceResponse GetNamespaceResponse::Copy() const {
	GetNamespaceResponse res;
	res._namespace = _namespace.Copy();
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string GetNamespaceResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto _namespace_val = obj.GetMember("namespace");
	if (!_namespace_val.IsValid()) {
		return "GetNamespaceResponse required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		if (properties_val.IsNull()) {
			//! do nothing, property is explicitly nullable
		} else {
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
						error = StringUtil::Format(
						    "GetNamespaceResponse property 'tmp' is not of type 'string', found %s instead",
						    json_utils::GetTypeDescription(val).c_str());
						return;
					}
					properties_tmp.emplace(key_str, std::move(tmp));
				});
				if (!error.empty()) {
					return error;
				}
			} else {
				return "GetNamespaceResponse property 'properties_tmp' is not of type 'object'";
			}
			properties = std::move(properties_tmp);
		}
	}
	return "";
}

void GetNamespaceResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: namespace
	auto _namespace_val = _namespace.ToJSON(writer);
	obj.Add("namespace", _namespace_val);

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
}

JSONMutableValue GetNamespaceResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
