#include "core/metadata/partition/iceberg_partition_constants.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

optional_ptr<const LogicalType> IcebergPartitionConstants::GetType(int32_t field_id, const IcebergTableSchema &schema,
                                                                   const IcebergTableMetadataSchemas &schemas) {
	auto column = schema.TryGetColumnByFieldId(field_id);
	if (!column) {
		column = schemas.FindColumnByFieldId(field_id);
	}
	return column ? optional_ptr<const LogicalType>(column->type) : nullptr;
}

unordered_map<int32_t, Value> IcebergPartitionConstants::Resolve(int32_t spec_id,
                                                                 const vector<IcebergPartitionInfo> &partition_values,
                                                                 const IcebergTableMetadata &metadata,
                                                                 const IcebergTableSchema &schema) {
	auto spec = metadata.partition_specs.find(spec_id);
	if (spec == metadata.partition_specs.end()) {
		throw InvalidConfigurationException("'partition_spec_id' %d doesn't exist in the metadata", spec_id);
	}
	unordered_map<int32_t, idx_t> field_indexes;
	for (idx_t i = 0; i < spec->second.fields.size(); i++) {
		field_indexes[spec->second.fields[i].source_id] = i;
	}
	unordered_map<int32_t, Value> constants;
	for (auto &item : field_indexes) {
		auto &field = spec->second.fields[item.second];
		if (field.transform != IcebergTransformType::IDENTITY) {
			continue;
		}
		auto type = GetType(item.first, schema, metadata.GetSchemas());
		if (!type) {
			continue;
		}
		for (auto &partition : partition_values) {
			if (partition.field_id == field.partition_field_id && !partition.value.IsNull()) {
				constants.emplace(item.first, IcebergValue::TransformPartitionValue(partition.value, *type));
				break;
			}
		}
	}
	return constants;
}

} // namespace duckdb
