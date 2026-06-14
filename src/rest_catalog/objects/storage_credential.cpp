
#include "rest_catalog/objects/storage_credential.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StorageCredential::StorageCredential() {
}

StorageCredential StorageCredential::FromJSON(yyjson_val *obj) {
	StorageCredential res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

StorageCredential StorageCredential::Copy() const {
	StorageCredential res;
	res.prefix = prefix;
	for (auto &entry : config) {
		res.config.emplace(entry.first, entry.second);
	}
	return res;
}

string StorageCredential::TryFromJSON(yyjson_val *obj) {
	string error;
	auto prefix_val = yyjson_obj_get(obj, "prefix");
	if (!prefix_val) {
		return "StorageCredential required property 'prefix' is missing";
	} else {
		if (yyjson_is_str(prefix_val)) {
			prefix = yyjson_get_str(prefix_val);
		} else {
			return StringUtil::Format("StorageCredential property 'prefix' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(prefix_val));
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (!config_val) {
		return "StorageCredential required property 'config' is missing";
	} else {
		if (yyjson_is_obj(config_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(config_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "StorageCredential property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config.emplace(key_str, std::move(tmp));
			}
		} else {
			return "StorageCredential property 'config' is not of type 'object'";
		}
	}
	return "";
}

void StorageCredential::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: prefix
	yyjson_mut_obj_add_str(doc, obj, "prefix", prefix.c_str());

	// Serialize: config
	yyjson_mut_val *config_obj = yyjson_mut_obj(doc);
	for (const auto &it : config) {
		auto &key = it.first;
		auto &value = it.second;
		yyjson_mut_obj_add_str(doc, config_obj, key.c_str(), value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "config", config_obj);
}

yyjson_mut_val *StorageCredential::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
