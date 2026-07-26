
#include "rest_catalog/objects/scan_report.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ScanReport::ScanReport() {
}

ScanReport ScanReport::FromJSON(JSONValue obj) {
	ScanReport res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ScanReport ScanReport::Copy() const {
	ScanReport res;
	res.table_name = table_name;
	res.snapshot_id = snapshot_id;
	res.filter = filter ? make_uniq<Expression>(filter->Copy()) : nullptr;
	res.schema_id = schema_id;
	res.projected_field_ids.reserve(projected_field_ids.size());
	for (auto &item : projected_field_ids) {
		res.projected_field_ids.emplace_back(item);
	}
	res.projected_field_names.reserve(projected_field_names.size());
	for (auto &item : projected_field_names) {
		res.projected_field_names.emplace_back(item);
	}
	res.metrics = metrics.Copy();
	if (metadata.has_value()) {
		res.metadata.emplace();
		for (auto &entry : (*metadata)) {
			(*res.metadata).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string ScanReport::TryFromJSON(JSONValue obj) {
	string error;
	auto table_name_val = obj.GetMember("table-name");
	if (!table_name_val.IsValid()) {
		return "ScanReport required property 'table-name' is missing";
	} else {
		if (json_utils::IsString(table_name_val)) {
			table_name = json_utils::GetString(table_name_val);
		} else {
			return StringUtil::Format("ScanReport property 'table_name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(table_name_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "ScanReport required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto filter_val = obj.GetMember("filter");
	if (!filter_val.IsValid()) {
		return "ScanReport required property 'filter' is missing";
	} else {
		filter = make_uniq<Expression>();
		error = filter->TryFromJSON(filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto schema_id_val = obj.GetMember("schema-id");
	if (!schema_id_val.IsValid()) {
		return "ScanReport required property 'schema-id' is missing";
	} else {
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'schema_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(schema_id_val).c_str());
		}
	}
	auto projected_field_ids_val = obj.GetMember("projected-field-ids");
	if (!projected_field_ids_val.IsValid()) {
		return "ScanReport required property 'projected-field-ids' is missing";
	} else {
		if (projected_field_ids_val.IsArray()) {
			projected_field_ids_val.IterateArray([&](JSONValue projected_field_ids_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t projected_field_ids_item;
				if (json_utils::IsInteger(projected_field_ids_item_val)) {
					projected_field_ids_item = json_utils::GetSignedInteger(projected_field_ids_item_val);
				} else {
					error = StringUtil::Format(
					    "ScanReport property 'projected_field_ids_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(projected_field_ids_item_val).c_str());
					return;
				}
				projected_field_ids.emplace_back(std::move(projected_field_ids_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_ids' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(projected_field_ids_val).c_str());
		}
	}
	auto projected_field_names_val = obj.GetMember("projected-field-names");
	if (!projected_field_names_val.IsValid()) {
		return "ScanReport required property 'projected-field-names' is missing";
	} else {
		if (projected_field_names_val.IsArray()) {
			projected_field_names_val.IterateArray([&](JSONValue projected_field_names_item_val) {
				if (!error.empty()) {
					return;
				}
				string projected_field_names_item;
				if (json_utils::IsString(projected_field_names_item_val)) {
					projected_field_names_item = json_utils::GetString(projected_field_names_item_val);
				} else {
					error = StringUtil::Format(
					    "ScanReport property 'projected_field_names_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(projected_field_names_item_val).c_str());
					return;
				}
				projected_field_names.emplace_back(std::move(projected_field_names_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_names' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(projected_field_names_val).c_str());
		}
	}
	auto metrics_val = obj.GetMember("metrics");
	if (!metrics_val.IsValid()) {
		return "ScanReport required property 'metrics' is missing";
	} else {
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_val = obj.GetMember("metadata");
	if (metadata_val.IsValid()) {
		case_insensitive_map_t<string> metadata_tmp;
		if (metadata_val.IsObject()) {
			metadata_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("ScanReport property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				metadata_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "ScanReport property 'metadata_tmp' is not of type 'object'";
		}
		metadata = std::move(metadata_tmp);
	}
	return "";
}

void ScanReport::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: table-name
	obj.AddString("table-name", table_name);

	// Serialize: snapshot-id
	obj.Add("snapshot-id", writer.CreateSignedInteger(snapshot_id));

	// Serialize: filter
	auto filter_val = filter->ToJSON(writer);
	obj.Add("filter", filter_val);

	// Serialize: schema-id
	obj.Add("schema-id", writer.CreateSignedInteger(schema_id));

	// Serialize: projected-field-ids
	auto projected_field_ids_arr = writer.CreateArray();
	for (const auto &item : projected_field_ids) {
		auto item_val = writer.CreateSignedInteger(item);
		projected_field_ids_arr.Append(item_val);
	}
	obj.Add("projected-field-ids", projected_field_ids_arr);

	// Serialize: projected-field-names
	auto projected_field_names_arr = writer.CreateArray();
	for (const auto &item : projected_field_names) {
		auto item_val = writer.CreateString(item);
		projected_field_names_arr.Append(item_val);
	}
	obj.Add("projected-field-names", projected_field_names_arr);

	// Serialize: metrics
	auto metrics_val = metrics.ToJSON(writer);
	obj.Add("metrics", metrics_val);

	// Serialize: metadata
	if (metadata.has_value()) {
		auto &metadata_value = *metadata;
		auto metadata_value_obj = writer.CreateObject();
		for (const auto &it : metadata_value) {
			auto &key = it.first;
			auto &value = it.second;
			metadata_value_obj.AddString(key, value);
		}
		obj.Add("metadata", metadata_value_obj);
	}
}

JSONMutableValue ScanReport::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
