
#include "rest_catalog/objects/load_view_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadViewResult::LoadViewResult() {
}

LoadViewResult LoadViewResult::FromJSON(yyjson_val *obj) {
	LoadViewResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadViewResult LoadViewResult::Copy() const {
	LoadViewResult res;
	res.metadata_location = metadata_location;
	res.metadata = metadata.Copy();
	if (config.has_value()) {
		res.config.emplace();
		for (auto &entry : (*config)) {
			(*res.config).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string LoadViewResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (!metadata_location_val) {
		return "LoadViewResult required property 'metadata-location' is missing";
	} else {
		if (yyjson_is_str(metadata_location_val)) {
			metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			return StringUtil::Format(
			    "LoadViewResult property 'metadata_location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(metadata_location_val));
		}
	}
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadViewResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (config_val) {
		case_insensitive_map_t<string> config_tmp;
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
					    "LoadViewResult property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "LoadViewResult property 'config_tmp' is not of type 'object'";
		}
		config = std::move(config_tmp);
	}
	return "";
}

void LoadViewResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: metadata-location
	yyjson_mut_obj_add_str(doc, obj, "metadata-location", metadata_location.c_str());

	// Serialize: metadata
	yyjson_mut_val *metadata_val = metadata.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "metadata", metadata_val);

	// Serialize: config
	if (config.has_value()) {
		auto &config_value = *config;
		yyjson_mut_val *config_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : config_value) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, config_value_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "config", config_value_obj);
	}
}

yyjson_mut_val *LoadViewResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
