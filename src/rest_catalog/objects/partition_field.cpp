
#include "rest_catalog/objects/partition_field.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PartitionField::PartitionField() {
}

PartitionField PartitionField::FromJSON(JSONValue obj) {
	PartitionField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PartitionField PartitionField::Copy() const {
	PartitionField res;
	res.source_id = source_id;
	res.transform = transform.Copy();
	res.name = name;
	if (field_id.has_value()) {
		res.field_id.emplace();
		(*res.field_id) = (*field_id);
	}
	return res;
}

string PartitionField::TryFromJSON(JSONValue obj) {
	string error;
	auto source_id_val = obj.GetMember("source-id");
	if (!source_id_val.IsValid()) {
		return "PartitionField required property 'source-id' is missing";
	} else {
		if (json_utils::IsInteger(source_id_val)) {
			source_id = json_utils::GetSignedInteger(source_id_val);
		} else {
			return StringUtil::Format("PartitionField property 'source_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(source_id_val).c_str());
		}
	}
	auto transform_val = obj.GetMember("transform");
	if (!transform_val.IsValid()) {
		return "PartitionField required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "PartitionField required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("PartitionField property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto field_id_val = obj.GetMember("field-id");
	if (field_id_val.IsValid()) {
		int32_t field_id_tmp;
		if (json_utils::IsInteger(field_id_val)) {
			field_id_tmp = json_utils::GetSignedInteger(field_id_val);
		} else {
			return StringUtil::Format(
			    "PartitionField property 'field_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(field_id_val).c_str());
		}
		field_id = std::move(field_id_tmp);
	}
	return "";
}

void PartitionField::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: source-id
	obj.Add("source-id", writer.CreateSignedInteger(source_id));

	// Serialize: transform
	auto transform_val = transform.ToJSON(writer);
	obj.Add("transform", transform_val);

	// Serialize: name
	obj.AddString("name", name);

	// Serialize: field-id
	if (field_id.has_value()) {
		auto &field_id_value = *field_id;
		obj.Add("field-id", writer.CreateSignedInteger(field_id_value));
	}
}

JSONMutableValue PartitionField::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
