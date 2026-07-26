
#include "rest_catalog/objects/catalog_config.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CatalogConfig::CatalogConfig() {
}

CatalogConfig CatalogConfig::FromJSON(JSONValue obj) {
	CatalogConfig res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CatalogConfig CatalogConfig::Copy() const {
	CatalogConfig res;
	for (auto &entry : defaults) {
		res.defaults.emplace(entry.first, entry.second);
	}
	for (auto &entry : overrides) {
		res.overrides.emplace(entry.first, entry.second);
	}
	if (endpoints.has_value()) {
		res.endpoints.emplace();
		(*res.endpoints).reserve((*endpoints).size());
		for (auto &item : (*endpoints)) {
			(*res.endpoints).emplace_back(item);
		}
	}
	if (idempotency_key_lifetime.has_value()) {
		res.idempotency_key_lifetime.emplace();
		(*res.idempotency_key_lifetime) = (*idempotency_key_lifetime);
	}
	return res;
}

string CatalogConfig::TryFromJSON(JSONValue obj) {
	string error;
	auto defaults_val = obj.GetMember("defaults");
	if (!defaults_val.IsValid()) {
		return "CatalogConfig required property 'defaults' is missing";
	} else {
		if (defaults_val.IsObject()) {
			defaults_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("CatalogConfig property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				defaults.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CatalogConfig property 'defaults' is not of type 'object'";
		}
	}
	auto overrides_val = obj.GetMember("overrides");
	if (!overrides_val.IsValid()) {
		return "CatalogConfig required property 'overrides' is missing";
	} else {
		if (overrides_val.IsObject()) {
			overrides_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("CatalogConfig property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				overrides.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CatalogConfig property 'overrides' is not of type 'object'";
		}
	}
	auto endpoints_val = obj.GetMember("endpoints");
	if (endpoints_val.IsValid()) {
		vector<string> endpoints_tmp;
		if (endpoints_val.IsArray()) {
			endpoints_val.IterateArray([&](JSONValue endpoints_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				string endpoints_tmp_item;
				if (json_utils::IsString(endpoints_tmp_item_val)) {
					endpoints_tmp_item = json_utils::GetString(endpoints_tmp_item_val);
				} else {
					error = StringUtil::Format(
					    "CatalogConfig property 'endpoints_tmp_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(endpoints_tmp_item_val).c_str());
					return;
				}
				endpoints_tmp.emplace_back(std::move(endpoints_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("CatalogConfig property 'endpoints_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(endpoints_val).c_str());
		}
		endpoints = std::move(endpoints_tmp);
	}
	auto idempotency_key_lifetime_val = obj.GetMember("idempotency-key-lifetime");
	if (idempotency_key_lifetime_val.IsValid()) {
		string idempotency_key_lifetime_tmp;
		if (json_utils::IsString(idempotency_key_lifetime_val)) {
			idempotency_key_lifetime_tmp = json_utils::GetString(idempotency_key_lifetime_val);
		} else {
			return StringUtil::Format(
			    "CatalogConfig property 'idempotency_key_lifetime_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(idempotency_key_lifetime_val).c_str());
		}
		idempotency_key_lifetime = std::move(idempotency_key_lifetime_tmp);
	}
	return "";
}

void CatalogConfig::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: defaults
	auto defaults_obj = writer.CreateObject();
	for (const auto &it : defaults) {
		auto &key = it.first;
		auto &value = it.second;
		defaults_obj.AddString(key, value);
	}
	obj.Add("defaults", defaults_obj);

	// Serialize: overrides
	auto overrides_obj = writer.CreateObject();
	for (const auto &it : overrides) {
		auto &key = it.first;
		auto &value = it.second;
		overrides_obj.AddString(key, value);
	}
	obj.Add("overrides", overrides_obj);

	// Serialize: endpoints
	if (endpoints.has_value()) {
		auto &endpoints_value = *endpoints;
		auto endpoints_value_arr = writer.CreateArray();
		for (const auto &item : endpoints_value) {
			auto item_val = writer.CreateString(item);
			endpoints_value_arr.Append(item_val);
		}
		obj.Add("endpoints", endpoints_value_arr);
	}

	// Serialize: idempotency-key-lifetime
	if (idempotency_key_lifetime.has_value()) {
		auto &idempotency_key_lifetime_value = *idempotency_key_lifetime;
		obj.AddString("idempotency-key-lifetime", idempotency_key_lifetime_value);
	}
}

JSONMutableValue CatalogConfig::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
