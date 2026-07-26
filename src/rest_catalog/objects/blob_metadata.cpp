
#include "rest_catalog/objects/blob_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

BlobMetadata::BlobMetadata() {
}

BlobMetadata BlobMetadata::FromJSON(JSONValue obj) {
	BlobMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

BlobMetadata BlobMetadata::Copy() const {
	BlobMetadata res;
	res.type = type;
	res.snapshot_id = snapshot_id;
	res.sequence_number = sequence_number;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string BlobMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "BlobMetadata required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("BlobMetadata property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "BlobMetadata required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("BlobMetadata property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto sequence_number_val = obj.GetMember("sequence-number");
	if (!sequence_number_val.IsValid()) {
		return "BlobMetadata required property 'sequence-number' is missing";
	} else {
		if (json_utils::IsInteger(sequence_number_val)) {
			sequence_number = json_utils::GetSignedInteger(sequence_number_val);
		} else if (json_utils::IsUnsignedInteger(sequence_number_val)) {
			sequence_number = json_utils::GetUnsignedInteger(sequence_number_val);
		} else {
			return StringUtil::Format(
			    "BlobMetadata property 'sequence_number' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(sequence_number_val).c_str());
		}
	}
	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsValid()) {
		return "BlobMetadata required property 'fields' is missing";
	} else {
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t fields_item;
				if (json_utils::IsInteger(fields_item_val)) {
					fields_item = json_utils::GetSignedInteger(fields_item_val);
				} else {
					error = StringUtil::Format(
					    "BlobMetadata property 'fields_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(fields_item_val).c_str());
					return;
				}
				fields.emplace_back(std::move(fields_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("BlobMetadata property 'fields' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
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
					error = StringUtil::Format("BlobMetadata property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "BlobMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void BlobMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	obj.AddString("type", type);

	// Serialize: snapshot-id
	obj.Add("snapshot-id", writer.CreateSignedInteger(snapshot_id));

	// Serialize: sequence-number
	obj.Add("sequence-number", writer.CreateSignedInteger(sequence_number));

	// Serialize: fields
	auto fields_arr = writer.CreateArray();
	for (const auto &item : fields) {
		auto item_val = writer.CreateSignedInteger(item);
		fields_arr.Append(item_val);
	}
	obj.Add("fields", fields_arr);

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

JSONMutableValue BlobMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
