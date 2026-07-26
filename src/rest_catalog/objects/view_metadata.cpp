
#include "rest_catalog/objects/view_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ViewMetadata::ViewMetadata() {
}

ViewMetadata ViewMetadata::FromJSON(JSONValue obj) {
	ViewMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewMetadata ViewMetadata::Copy() const {
	ViewMetadata res;
	res.view_uuid = view_uuid;
	res.format_version = format_version;
	res.location = location;
	res.current_version_id = current_version_id;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.version_log.reserve(version_log.size());
	for (auto &item : version_log) {
		res.version_log.emplace_back(item.Copy());
	}
	res.schemas.reserve(schemas.size());
	for (auto &item : schemas) {
		res.schemas.emplace_back(item.Copy());
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string ViewMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto view_uuid_val = obj.GetMember("view-uuid");
	if (!view_uuid_val.IsValid()) {
		return "ViewMetadata required property 'view-uuid' is missing";
	} else {
		if (json_utils::IsString(view_uuid_val)) {
			view_uuid = json_utils::GetString(view_uuid_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'view_uuid' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(view_uuid_val).c_str());
		}
	}
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "ViewMetadata required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	auto location_val = obj.GetMember("location");
	if (!location_val.IsValid()) {
		return "ViewMetadata required property 'location' is missing";
	} else {
		if (json_utils::IsString(location_val)) {
			location = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format("ViewMetadata property 'location' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(location_val).c_str());
		}
	}
	auto current_version_id_val = obj.GetMember("current-version-id");
	if (!current_version_id_val.IsValid()) {
		return "ViewMetadata required property 'current-version-id' is missing";
	} else {
		if (json_utils::IsInteger(current_version_id_val)) {
			current_version_id = json_utils::GetSignedInteger(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "ViewMetadata property 'current_version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_version_id_val).c_str());
		}
	}
	auto versions_val = obj.GetMember("versions");
	if (!versions_val.IsValid()) {
		return "ViewMetadata required property 'versions' is missing";
	} else {
		if (versions_val.IsArray()) {
			versions_val.IterateArray([&](JSONValue versions_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewVersion versions_item;
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
			return StringUtil::Format("ViewMetadata property 'versions' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(versions_val).c_str());
		}
	}
	auto version_log_val = obj.GetMember("version-log");
	if (!version_log_val.IsValid()) {
		return "ViewMetadata required property 'version-log' is missing";
	} else {
		if (version_log_val.IsArray()) {
			version_log_val.IterateArray([&](JSONValue version_log_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewHistoryEntry version_log_item;
				error = version_log_item.TryFromJSON(version_log_item_val);
				if (!error.empty()) {
					return;
				}
				version_log.emplace_back(std::move(version_log_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'version_log' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(version_log_val).c_str());
		}
	}
	auto schemas_val = obj.GetMember("schemas");
	if (!schemas_val.IsValid()) {
		return "ViewMetadata required property 'schemas' is missing";
	} else {
		if (schemas_val.IsArray()) {
			schemas_val.IterateArray([&](JSONValue schemas_item_val) {
				if (!error.empty()) {
					return;
				}
				Schema schemas_item;
				error = schemas_item.TryFromJSON(schemas_item_val);
				if (!error.empty()) {
					return;
				}
				schemas.emplace_back(std::move(schemas_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ViewMetadata property 'schemas' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(schemas_val).c_str());
		}
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
					error = StringUtil::Format("ViewMetadata property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "ViewMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void ViewMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: view-uuid
	obj.AddString("view-uuid", view_uuid);

	// Serialize: format-version
	obj.Add("format-version", writer.CreateSignedInteger(format_version));

	// Serialize: location
	obj.AddString("location", location);

	// Serialize: current-version-id
	obj.Add("current-version-id", writer.CreateSignedInteger(current_version_id));

	// Serialize: versions
	auto versions_arr = writer.CreateArray();
	for (const auto &item : versions) {
		auto item_val = item.ToJSON(writer);
		versions_arr.Append(item_val);
	}
	obj.Add("versions", versions_arr);

	// Serialize: version-log
	auto version_log_arr = writer.CreateArray();
	for (const auto &item : version_log) {
		auto item_val = item.ToJSON(writer);
		version_log_arr.Append(item_val);
	}
	obj.Add("version-log", version_log_arr);

	// Serialize: schemas
	auto schemas_arr = writer.CreateArray();
	for (const auto &item : schemas) {
		auto item_val = item.ToJSON(writer);
		schemas_arr.Append(item_val);
	}
	obj.Add("schemas", schemas_arr);

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

JSONMutableValue ViewMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
