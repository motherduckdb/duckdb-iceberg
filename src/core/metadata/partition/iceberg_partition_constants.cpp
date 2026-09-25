#include "core/metadata/partition/iceberg_partition_constants.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

unordered_map<int32_t, Value>
IcebergPartitionConstants::Resolve(int32_t spec_id, const unordered_map<int32_t, IcebergPartitionSpec> &specs,
                                   const vector<IcebergPartitionInfo> &partition_values,
                                   const TypeLookup &lookup_type) {
	auto spec = specs.find(spec_id);
	if (spec == specs.end()) {
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
		auto type = lookup_type(item.first);
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
