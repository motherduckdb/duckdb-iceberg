
#include "rest_catalog/objects/encrypted_key.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EncryptedKey::EncryptedKey() {
}

EncryptedKey EncryptedKey::FromJSON(yyjson_val *obj) {
	EncryptedKey res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

EncryptedKey EncryptedKey::Copy() const {
	EncryptedKey res;
	res.key_id = key_id;
	res.encrypted_key_metadata = encrypted_key_metadata;
	if (encrypted_by_id.has_value()) {
		res.encrypted_by_id.emplace();
		(*res.encrypted_by_id) = (*encrypted_by_id);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string EncryptedKey::TryFromJSON(yyjson_val *obj) {
	string error;
	auto key_id_val = yyjson_obj_get(obj, "key-id");
	if (!key_id_val) {
		return "EncryptedKey required property 'key-id' is missing";
	} else {
		if (yyjson_is_str(key_id_val)) {
			key_id = yyjson_get_str(key_id_val);
		} else {
			return StringUtil::Format("EncryptedKey property 'key_id' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(key_id_val));
		}
	}
	auto encrypted_key_metadata_val = yyjson_obj_get(obj, "encrypted-key-metadata");
	if (!encrypted_key_metadata_val) {
		return "EncryptedKey required property 'encrypted-key-metadata' is missing";
	} else {
		if (yyjson_is_str(encrypted_key_metadata_val)) {
			encrypted_key_metadata = yyjson_get_str(encrypted_key_metadata_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_key_metadata' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(encrypted_key_metadata_val));
		}
	}
	auto encrypted_by_id_val = yyjson_obj_get(obj, "encrypted-by-id");
	if (encrypted_by_id_val) {
		string encrypted_by_id_tmp;
		if (yyjson_is_str(encrypted_by_id_val)) {
			encrypted_by_id_tmp = yyjson_get_str(encrypted_by_id_val);
		} else {
			return StringUtil::Format(
			    "EncryptedKey property 'encrypted_by_id_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(encrypted_by_id_val));
		}
		encrypted_by_id = std::move(encrypted_by_id_tmp);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("EncryptedKey property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "EncryptedKey property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void EncryptedKey::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: key-id
	yyjson_mut_obj_add_str(doc, obj, "key-id", key_id.c_str());

	// Serialize: encrypted-key-metadata
	yyjson_mut_obj_add_str(doc, obj, "encrypted-key-metadata", encrypted_key_metadata.c_str());

	// Serialize: encrypted-by-id
	if (encrypted_by_id.has_value()) {
		auto &encrypted_by_id_value = *encrypted_by_id;
		yyjson_mut_obj_add_str(doc, obj, "encrypted-by-id", encrypted_by_id_value.c_str());
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, properties_value_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}
}

yyjson_mut_val *EncryptedKey::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
