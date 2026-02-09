
#include "rest_catalog/objects/load_table_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadTableResult LoadTableResult::FromJSON(yyjson_val *obj) {
	LoadTableResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string LoadTableResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadTableResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (metadata_location_val) {
		has_metadata_location = true;
		if (yyjson_is_null(metadata_location_val)) {
			//! do nothing, property is explicitly nullable
		} else if (yyjson_is_str(metadata_location_val)) {
			metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadTableResult property 'metadata_location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(metadata_location_val));
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (config_val) {
		has_config = true;
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
					    "LoadTableResult property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config.emplace(key_str, std::move(tmp));
			}
		} else {
			return "LoadTableResult property 'config' is not of type 'object'";
		}
	}
	auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
	if (storage_credentials_val) {
		has_storage_credentials = true;
		if (yyjson_is_arr(storage_credentials_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				StorageCredential tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "LoadTableResult property 'storage_credentials' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
	}
	return "";
}

yyjson_mut_val *LoadTableResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: metadata
	yyjson_mut_val *metadata_val = metadata.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "metadata", metadata_val);

	// Serialize: metadata-location
	if (has_metadata_location) {
		yyjson_mut_obj_add_str(doc, obj, "metadata-location", metadata_location.c_str());
	}

	// Serialize: config
	if (has_config) {
		yyjson_mut_val *config_obj = yyjson_mut_obj(doc);
		for (const auto &it : config) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, config_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "config", config_obj);
	}

	// Serialize: storage-credentials
	if (has_storage_credentials) {
		yyjson_mut_val *storage_credentials_arr = yyjson_mut_arr(doc);
		for (const auto &item : storage_credentials) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(storage_credentials_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "storage-credentials", storage_credentials_arr);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
