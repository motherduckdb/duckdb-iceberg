
#include "rest_catalog/objects/catalog_config.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CatalogConfig::CatalogConfig() {
}

CatalogConfig CatalogConfig::FromJSON(yyjson_val *obj) {
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
	if (has_endpoints) {
		res.endpoints.reserve(endpoints.size());
		for (auto &item : endpoints) {
			res.endpoints.emplace_back(item);
		}
	}
	res.has_endpoints = has_endpoints;
	if (has_idempotency_key_lifetime) {
		res.idempotency_key_lifetime = idempotency_key_lifetime;
	}
	res.has_idempotency_key_lifetime = has_idempotency_key_lifetime;
	return res;
}

string CatalogConfig::TryFromJSON(yyjson_val *obj) {
	string error;
	auto defaults_val = yyjson_obj_get(obj, "defaults");
	if (!defaults_val) {
		return "CatalogConfig required property 'defaults' is missing";
	} else {
		if (yyjson_is_obj(defaults_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(defaults_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				defaults.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CatalogConfig property 'defaults' is not of type 'object'";
		}
	}
	auto overrides_val = yyjson_obj_get(obj, "overrides");
	if (!overrides_val) {
		return "CatalogConfig required property 'overrides' is missing";
	} else {
		if (yyjson_is_obj(overrides_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(overrides_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				overrides.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CatalogConfig property 'overrides' is not of type 'object'";
		}
	}
	auto endpoints_val = yyjson_obj_get(obj, "endpoints");
	if (endpoints_val && !yyjson_is_null(endpoints_val)) {
		has_endpoints = true;
		if (yyjson_is_arr(endpoints_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(endpoints_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				endpoints.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("CatalogConfig property 'endpoints' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(endpoints_val));
		}
	}
	auto idempotency_key_lifetime_val = yyjson_obj_get(obj, "idempotency-key-lifetime");
	if (idempotency_key_lifetime_val && !yyjson_is_null(idempotency_key_lifetime_val)) {
		has_idempotency_key_lifetime = true;
		if (yyjson_is_str(idempotency_key_lifetime_val)) {
			idempotency_key_lifetime = yyjson_get_str(idempotency_key_lifetime_val);
		} else {
			return StringUtil::Format(
			    "CatalogConfig property 'idempotency_key_lifetime' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(idempotency_key_lifetime_val));
		}
	}
	return "";
}

yyjson_mut_val *CatalogConfig::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: defaults
	yyjson_mut_val *defaults_obj = yyjson_mut_obj(doc);
	for (const auto &it : defaults) {
		auto &key = it.first;
		auto &value = it.second;
		yyjson_mut_obj_add_str(doc, defaults_obj, key.c_str(), value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "defaults", defaults_obj);

	// Serialize: overrides
	yyjson_mut_val *overrides_obj = yyjson_mut_obj(doc);
	for (const auto &it : overrides) {
		auto &key = it.first;
		auto &value = it.second;
		yyjson_mut_obj_add_str(doc, overrides_obj, key.c_str(), value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "overrides", overrides_obj);

	// Serialize: endpoints
	if (has_endpoints) {
		yyjson_mut_val *endpoints_arr = yyjson_mut_arr(doc);
		for (const auto &item : endpoints) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(endpoints_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "endpoints", endpoints_arr);
	}

	// Serialize: idempotency-key-lifetime
	if (has_idempotency_key_lifetime) {
		yyjson_mut_obj_add_str(doc, obj, "idempotency-key-lifetime", idempotency_key_lifetime.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
