
#include "rest_catalog/objects/snapshot.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Snapshot::Snapshot() {
}
Snapshot::Object2::Object2() {
}

Snapshot::Object2 Snapshot::Object2::FromJSON(JSONValue obj) {
	Object2 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Snapshot::Object2 Snapshot::Object2::Copy() const {
	Object2 res;
	res.operation = operation;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second);
	}
	return res;
}

string Snapshot::Object2::TryFromJSON(JSONValue obj) {
	string error;
	auto operation_val = obj.GetMember("operation");
	if (!operation_val.IsValid()) {
		return "Object2 required property 'operation' is missing";
	} else {
		if (json_utils::IsString(operation_val)) {
			operation = json_utils::GetString(operation_val);
		} else {
			return StringUtil::Format("Object2 property 'operation' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(operation_val).c_str());
		}
	}
	case_insensitive_set_t handled_properties {"operation"};
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		if (handled_properties.count(key_str)) {
			return;
		}
		string tmp;
		if (json_utils::IsString(val)) {
			tmp = json_utils::GetString(val);
		} else {
			error = StringUtil::Format("Object2 property 'tmp' is not of type 'string', found %s instead",
			                           json_utils::GetTypeDescription(val).c_str());
			return;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	});
	if (!error.empty()) {
		return error;
	}
	return "";
}

void Snapshot::Object2::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: operation
	auto operation_json = writer.CreateString(operation);
	obj.Add("operation", operation_json);

	// Serialize additional properties
	for (const auto &[key, value] : additional_properties) {
		auto value_json = writer.CreateString(value);
		obj.Add(key, value_json);
	}
}

JSONMutableValue Snapshot::Object2::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

Snapshot Snapshot::FromJSON(JSONValue obj) {
	Snapshot res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Snapshot Snapshot::Copy() const {
	Snapshot res;
	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms;
	res.manifest_list = manifest_list;
	res.summary = summary.Copy();
	if (parent_snapshot_id.has_value()) {
		res.parent_snapshot_id.emplace();
		(*res.parent_snapshot_id) = (*parent_snapshot_id);
	}
	if (sequence_number.has_value()) {
		res.sequence_number.emplace();
		(*res.sequence_number) = (*sequence_number);
	}
	if (first_row_id.has_value()) {
		res.first_row_id.emplace();
		(*res.first_row_id) = (*first_row_id);
	}
	if (added_rows.has_value()) {
		res.added_rows.emplace();
		(*res.added_rows) = (*added_rows);
	}
	if (schema_id.has_value()) {
		res.schema_id.emplace();
		(*res.schema_id) = (*schema_id);
	}
	return res;
}

string Snapshot::TryFromJSON(JSONValue obj) {
	string error;
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "Snapshot required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("Snapshot property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "Snapshot required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format("Snapshot property 'timestamp_ms' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	auto manifest_list_val = obj.GetMember("manifest-list");
	if (!manifest_list_val.IsValid()) {
		return "Snapshot required property 'manifest-list' is missing";
	} else {
		if (json_utils::IsString(manifest_list_val)) {
			manifest_list = json_utils::GetString(manifest_list_val);
		} else {
			return StringUtil::Format("Snapshot property 'manifest_list' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(manifest_list_val).c_str());
		}
	}
	auto summary_val = obj.GetMember("summary");
	if (!summary_val.IsValid()) {
		return "Snapshot required property 'summary' is missing";
	} else {
		error = summary.TryFromJSON(summary_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto parent_snapshot_id_val = obj.GetMember("parent-snapshot-id");
	if (parent_snapshot_id_val.IsValid()) {
		int64_t parent_snapshot_id_tmp;
		if (json_utils::IsInteger(parent_snapshot_id_val)) {
			parent_snapshot_id_tmp = json_utils::GetSignedInteger(parent_snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(parent_snapshot_id_val)) {
			parent_snapshot_id_tmp = json_utils::GetUnsignedInteger(parent_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "Snapshot property 'parent_snapshot_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(parent_snapshot_id_val).c_str());
		}
		parent_snapshot_id = std::move(parent_snapshot_id_tmp);
	}
	auto sequence_number_val = obj.GetMember("sequence-number");
	if (sequence_number_val.IsValid()) {
		int64_t sequence_number_tmp;
		if (json_utils::IsInteger(sequence_number_val)) {
			sequence_number_tmp = json_utils::GetSignedInteger(sequence_number_val);
		} else if (json_utils::IsUnsignedInteger(sequence_number_val)) {
			sequence_number_tmp = json_utils::GetUnsignedInteger(sequence_number_val);
		} else {
			return StringUtil::Format(
			    "Snapshot property 'sequence_number_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(sequence_number_val).c_str());
		}
		sequence_number = std::move(sequence_number_tmp);
	}
	auto first_row_id_val = obj.GetMember("first-row-id");
	if (first_row_id_val.IsValid()) {
		int64_t first_row_id_tmp;
		if (json_utils::IsInteger(first_row_id_val)) {
			first_row_id_tmp = json_utils::GetSignedInteger(first_row_id_val);
		} else if (json_utils::IsUnsignedInteger(first_row_id_val)) {
			first_row_id_tmp = json_utils::GetUnsignedInteger(first_row_id_val);
		} else {
			return StringUtil::Format("Snapshot property 'first_row_id_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(first_row_id_val).c_str());
		}
		first_row_id = std::move(first_row_id_tmp);
	}
	auto added_rows_val = obj.GetMember("added-rows");
	if (added_rows_val.IsValid()) {
		int64_t added_rows_tmp;
		if (json_utils::IsInteger(added_rows_val)) {
			added_rows_tmp = json_utils::GetSignedInteger(added_rows_val);
		} else if (json_utils::IsUnsignedInteger(added_rows_val)) {
			added_rows_tmp = json_utils::GetUnsignedInteger(added_rows_val);
		} else {
			return StringUtil::Format("Snapshot property 'added_rows_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(added_rows_val).c_str());
		}
		added_rows = std::move(added_rows_tmp);
	}
	auto schema_id_val = obj.GetMember("schema-id");
	if (schema_id_val.IsValid()) {
		int32_t schema_id_tmp;
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id_tmp = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format("Snapshot property 'schema_id_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(schema_id_val).c_str());
		}
		schema_id = std::move(schema_id_tmp);
	}
	return "";
}

void Snapshot::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);

	// Serialize: manifest-list
	auto manifest_list_json = writer.CreateString(manifest_list);
	obj.Add("manifest-list", manifest_list_json);

	// Serialize: summary
	auto summary_json = summary.ToJSON(writer);
	obj.Add("summary", summary_json);

	// Serialize: parent-snapshot-id
	if (parent_snapshot_id.has_value()) {
		auto &parent_snapshot_id_value = *parent_snapshot_id;
		auto parent_snapshot_id_json = writer.CreateSignedInteger(parent_snapshot_id_value);
		obj.Add("parent-snapshot-id", parent_snapshot_id_json);
	}

	// Serialize: sequence-number
	if (sequence_number.has_value()) {
		auto &sequence_number_value = *sequence_number;
		auto sequence_number_json = writer.CreateSignedInteger(sequence_number_value);
		obj.Add("sequence-number", sequence_number_json);
	}

	// Serialize: first-row-id
	if (first_row_id.has_value()) {
		auto &first_row_id_value = *first_row_id;
		auto first_row_id_json = writer.CreateSignedInteger(first_row_id_value);
		obj.Add("first-row-id", first_row_id_json);
	}

	// Serialize: added-rows
	if (added_rows.has_value()) {
		auto &added_rows_value = *added_rows;
		auto added_rows_json = writer.CreateSignedInteger(added_rows_value);
		obj.Add("added-rows", added_rows_json);
	}

	// Serialize: schema-id
	if (schema_id.has_value()) {
		auto &schema_id_value = *schema_id;
		auto schema_id_json = writer.CreateSignedInteger(schema_id_value);
		obj.Add("schema-id", schema_id_json);
	}
}

JSONMutableValue Snapshot::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
