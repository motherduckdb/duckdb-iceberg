#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

IcebergPartitionSpecField IcebergPartitionSpecField::ParseFromJson(const rest_api_objects::PartitionField &field) {
	IcebergPartitionSpecField result;

	result.name = field.name;
	result.transform = field.transform.value;
	result.source_id = field.source_id;
	D_ASSERT(field.field_id);
	result.partition_field_id = *field.field_id;
	return result;
}

bool IcebergPartitionSpec::Equals(const IcebergPartitionSpec &other) const {
	if (other.fields.size() != fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.fields.size(); i++) {
		auto existing_partition_col_source_id = other.fields[i].source_id;

		//! Compare source ids
		auto new_spec_col_source_id = fields[i].source_id;
		if (existing_partition_col_source_id != new_spec_col_source_id) {
			return false;
		}

		//! Compare transforms
		auto existing_partition_col_transform = other.fields[i].transform.RawType();
		auto new_spec_col_transform = fields[i].transform.RawType();
		if (existing_partition_col_transform != new_spec_col_transform) {
			return false;
		}
	}
	return true;
}

IcebergPartitionSpec IcebergPartitionSpec::ParseFromJson(const rest_api_objects::PartitionSpec &partition_spec) {
	D_ASSERT(partition_spec.spec_id);
	IcebergPartitionSpec result(*partition_spec.spec_id);
	for (auto &field : partition_spec.fields) {
		result.fields.push_back(IcebergPartitionSpecField::ParseFromJson(field));
	}
	return result;
}

bool IcebergPartitionSpec::IsPartitioned() const {
	//! A partition spec is considered partitioned if it has at least one field that doesn't have a 'void' transform
	for (const auto &field : fields) {
		if (field.transform != IcebergTransformType::VOID) {
			return true;
		}
	}

	return false;
}

bool IcebergPartitionSpec::IsUnpartitioned() const {
	return !IsPartitioned();
}

const vector<IcebergPartitionSpecField> &IcebergPartitionSpec::GetFields() const {
	return fields;
}

void IcebergPartitionSpecField::SetPartitionSpecFieldName(const string &column_name) {
	string transform_raw_type = transform.RawType();
	for (idx_t i = 0; i < transform_raw_type.size(); i++) {
		char c = transform_raw_type[i];
		bool valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
		if (!valid) {
			transform_raw_type[i] = '_';
		}
	}
	// Avro names must not start with a digit
	if (!transform_raw_type.empty() && transform_raw_type[0] >= '0' && transform_raw_type[0] <= '9') {
		transform_raw_type = "_" + transform_raw_type;
	}
	name = transform_raw_type + "_" + column_name + "_" + to_string(source_id);
}

const string &IcebergPartitionSpecField::GetPartitionSpecFieldName() const {
	return name;
}

optional_ptr<const IcebergPartitionSpecField> IcebergPartitionSpec::TryGetFieldBySourceId(idx_t source_id) const {
	for (auto &field : fields) {
		if (field.source_id == source_id) {
			return field;
		}
	}
	return nullptr;
}

const IcebergPartitionSpecField &IcebergPartitionSpec::GetFieldBySourceId(idx_t source_id) const {
	auto res = TryGetFieldBySourceId(source_id);
	if (!res) {
		throw InvalidConfigurationException("Field with source_id %d doesn't exist in this partition spec (id %d)",
		                                    source_id, spec_id);
	}
	return *res;
}

JSONMutableValue IcebergPartitionSpec::FieldsToJSON(JSONWriter &writer) const {
	auto fields_array = writer.CreateArray();
	for (auto &field : fields) {
		auto field_obj = writer.CreateObject();
		fields_array.Append(field_obj);
		field_obj.AddString("name", field.GetPartitionSpecFieldName());
		field_obj.AddString("transform", field.transform.RawType());
		field_obj.Add("source-id", writer.CreateUnsignedInteger(field.source_id));
		field_obj.Add("field-id", writer.CreateUnsignedInteger(field.partition_field_id));
	}
	return fields_array;
}

string IcebergPartitionSpec::FieldsToJSONString() const {
	JSONWriter writer;
	writer.SetRoot(FieldsToJSON(writer));
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
}

JSONMutableValue IcebergPartitionSpec::ToJSON(JSONWriter &writer) const {
	auto partition_obj = writer.CreateObject();
	partition_obj.Add("spec-id", writer.CreateSignedInteger(spec_id));
	partition_obj.Add("fields", FieldsToJSON(writer));
	return partition_obj;
}

} // namespace duckdb
