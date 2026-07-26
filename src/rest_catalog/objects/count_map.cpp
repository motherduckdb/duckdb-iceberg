
#include "rest_catalog/objects/count_map.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CountMap::CountMap() {
}

CountMap CountMap::FromJSON(JSONValue obj) {
	CountMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CountMap CountMap::Copy() const {
	CountMap res;
	if (keys.has_value()) {
		res.keys.emplace();
		(*res.keys).reserve((*keys).size());
		for (auto &item : (*keys)) {
			(*res.keys).emplace_back(item.Copy());
		}
	}
	if (values.has_value()) {
		res.values.emplace();
		(*res.values).reserve((*values).size());
		for (auto &item : (*values)) {
			(*res.values).emplace_back(item.Copy());
		}
	}
	return res;
}

string CountMap::TryFromJSON(JSONValue obj) {
	string error;
	auto keys_val = obj.GetMember("keys");
	if (keys_val.IsValid()) {
		vector<IntegerTypeValue> keys_tmp;
		if (keys_val.IsArray()) {
			keys_val.IterateArray([&](JSONValue keys_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				IntegerTypeValue keys_tmp_item;
				error = keys_tmp_item.TryFromJSON(keys_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				keys_tmp.emplace_back(std::move(keys_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("CountMap property 'keys_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(keys_val).c_str());
		}
		keys = std::move(keys_tmp);
	}
	auto values_val = obj.GetMember("values");
	if (values_val.IsValid()) {
		vector<LongTypeValue> values_tmp;
		if (values_val.IsArray()) {
			values_val.IterateArray([&](JSONValue values_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				LongTypeValue values_tmp_item;
				error = values_tmp_item.TryFromJSON(values_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				values_tmp.emplace_back(std::move(values_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("CountMap property 'values_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(values_val).c_str());
		}
		values = std::move(values_tmp);
	}
	return "";
}

void CountMap::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: keys
	if (keys.has_value()) {
		auto &keys_value = *keys;
		auto keys_value_arr = writer.CreateArray();
		for (const auto &item : keys_value) {
			auto item_val = item.ToJSON(writer);
			keys_value_arr.Append(item_val);
		}
		obj.Add("keys", keys_value_arr);
	}

	// Serialize: values
	if (values.has_value()) {
		auto &values_value = *values;
		auto values_value_arr = writer.CreateArray();
		for (const auto &item : values_value) {
			auto item_val = item.ToJSON(writer);
			values_value_arr.Append(item_val);
		}
		obj.Add("values", values_value_arr);
	}
}

JSONMutableValue CountMap::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
