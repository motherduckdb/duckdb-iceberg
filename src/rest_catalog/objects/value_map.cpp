
#include "rest_catalog/objects/value_map.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ValueMap::ValueMap() {
}

ValueMap ValueMap::FromJSON(yyjson_val *obj) {
	ValueMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ValueMap ValueMap::Copy() const {
	ValueMap res;
	if (has_keys) {
		res.keys.reserve(keys.size());
		for (auto &item : keys) {
			res.keys.emplace_back(item.Copy());
		}
	}
	res.has_keys = has_keys;
	if (has_values) {
		res.values.reserve(values.size());
		for (auto &item : values) {
			res.values.emplace_back(item.Copy());
		}
	}
	res.has_values = has_values;
	return res;
}

string ValueMap::TryFromJSON(yyjson_val *obj) {
	string error;
	auto keys_val = yyjson_obj_get(obj, "keys");
	if (keys_val && !yyjson_is_null(keys_val)) {
		has_keys = true;
		if (yyjson_is_arr(keys_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(keys_val, idx, max, val) {
				IntegerTypeValue tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				keys.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ValueMap property 'keys' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(keys_val));
		}
	}
	auto values_val = yyjson_obj_get(obj, "values");
	if (values_val && !yyjson_is_null(values_val)) {
		has_values = true;
		if (yyjson_is_arr(values_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(values_val, idx, max, val) {
				PrimitiveTypeValue tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				values.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ValueMap property 'values' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(values_val));
		}
	}
	return "";
}

void ValueMap::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: keys
	if (has_keys) {
		yyjson_mut_val *keys_arr = yyjson_mut_arr(doc);
		for (const auto &item : keys) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(keys_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "keys", keys_arr);
	}

	// Serialize: values
	if (has_values) {
		yyjson_mut_val *values_arr = yyjson_mut_arr(doc);
		for (const auto &item : values) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(values_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "values", values_arr);
	}
}

yyjson_mut_val *ValueMap::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
