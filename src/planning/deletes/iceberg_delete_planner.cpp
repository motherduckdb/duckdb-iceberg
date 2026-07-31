#include "planning/deletes/iceberg_delete_planner.hpp"

#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"

namespace duckdb {

static bool DeleteManifestMatchesDataFile(const IcebergDeletePlanningContext &context,
                                          const IcebergManifestFile &delete_manifest,
                                          const BoundIcebergManifestEntry &data_manifest_entry) {
	auto partition_spec_it = context.metadata.partition_specs.find(delete_manifest.partition_spec_id);
	if (partition_spec_it == context.metadata.partition_specs.end()) {
		throw InvalidInputException("Delete manifest %s references partition_spec_id %d which doesn't exist",
		                            delete_manifest.manifest_path, delete_manifest.partition_spec_id);
	}
	auto &delete_partition_spec = partition_spec_it->second;
	if (delete_partition_spec.IsUnpartitioned()) {
		return true;
	}

	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	if (delete_manifest.partition_spec_id != data_manifest.partition_spec_id) {
		return false;
	}
	if (!delete_manifest.partitions.has_partitions) {
		return true;
	}

	auto &field_summaries = delete_manifest.partitions.field_summary;
	if (delete_partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException("Delete manifest has %d partition summaries but partition spec %d has %d fields",
		                            field_summaries.size(), delete_manifest.partition_spec_id,
		                            delete_partition_spec.fields.size());
	}

	auto &data_file = data_manifest_entry.entry.data_file;
	unordered_map<uint64_t, reference<const Value>> partition_values;
	for (auto &partition : data_file.partition_info) {
		partition_values.emplace(partition.field_id, partition.value);
	}

	for (idx_t field_idx = 0; field_idx < delete_partition_spec.fields.size(); field_idx++) {
		auto &field = delete_partition_spec.fields[field_idx];
		auto partition_value_it = partition_values.find(field.partition_field_id);
		if (partition_value_it == partition_values.end()) {
			return true;
		}
		auto &partition_value = partition_value_it->second.get();
		auto &field_summary = field_summaries[field_idx];
		if (partition_value.IsNull()) {
			if (!field_summary.contains_null) {
				return false;
			}
			continue;
		}

		auto source_column = context.metadata.FindColumnByFieldId(NumericCast<int32_t>(field.source_id));
		if (!source_column) {
			return true;
		}
		auto partition_type = field.transform.GetSerializedType(source_column->type);
		auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
		                                                      source_column->name, partition_type);
		auto typed_partition_value = partition_value.DefaultCastAs(partition_type);
		if (stats.lower_bound && typed_partition_value < *stats.lower_bound) {
			return false;
		}
		if (stats.upper_bound && typed_partition_value > *stats.upper_bound) {
			return false;
		}
	}
	return true;
}

vector<idx_t>
IcebergDeletePlanner::GetDeleteManifestsForDataFile(const IcebergDeletePlanningContext &context,
                                                    const BoundIcebergManifestEntry &data_manifest_entry) {
	vector<idx_t> result;
	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	auto data_sequence_number = data_manifest_entry.entry.GetSequenceNumber(data_manifest);
	for (idx_t manifest_idx = 0; manifest_idx < context.delete_manifests.size(); manifest_idx++) {
		if (!context.delete_manifest_matches[manifest_idx]) {
			continue;
		}
		auto &delete_manifest = context.delete_manifests[manifest_idx].entry.file;
		if (!delete_manifest.sequence_number) {
			throw InvalidConfigurationException("Delete manifest %s does not have a sequence number",
			                                    delete_manifest.manifest_path);
		}
		if (*delete_manifest.sequence_number < data_sequence_number) {
			continue;
		}
		if (!DeleteManifestMatchesDataFile(context, delete_manifest, data_manifest_entry)) {
			continue;
		}
		result.push_back(manifest_idx);
	}
	return result;
}

vector<reference<const IcebergEqualityDeleteFile>>
IcebergDeletePlanner::GetEqualityDeletesForFile(const IcebergDeletePlanningContext &context,
                                                const BoundIcebergManifestEntry &bound_manifest_entry) {
	vector<reference<const IcebergEqualityDeleteFile>> result;
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &manifest_file = context.data_manifests[bound_manifest_entry.manifest_file_idx].entry.file;
	auto &data_file = manifest_entry.data_file;
	auto &delete_entries = context.provider.DeleteManifestEntries();
	auto &equality_delete_data = context.provider.EqualityDeleteData();
	auto it = equality_delete_data.upper_bound(manifest_entry.GetSequenceNumber(manifest_file));
	for (; it != equality_delete_data.end(); it++) {
		for (auto &delete_file_ptr : it->second) {
			auto &delete_file = *delete_file_ptr;
			auto manifest_entry_index = delete_file.manifest_entry_index;
			if (manifest_entry_index >= delete_entries.size()) {
				throw InternalException("Delete manifest entry index %llu is out of bounds for %llu entries",
				                        manifest_entry_index, delete_entries.size());
			}
			auto &delete_manifest_entry = delete_entries[manifest_entry_index];
			auto &delete_data_file = delete_manifest_entry.entry.data_file;
			if (!context.provider.DeleteFileAppliesToDataFile(data_file.file_path, delete_data_file.file_path)) {
				continue;
			}
			auto &delete_manifest_file = context.delete_manifests[delete_manifest_entry.manifest_file_idx].entry.file;
			auto delete_partition_spec_id = delete_manifest_file.partition_spec_id;
			auto &partition_spec = context.metadata.partition_specs.at(delete_partition_spec_id);
			if (partition_spec.IsPartitioned()) {
				if (delete_partition_spec_id != manifest_file.partition_spec_id) {
					continue;
				}
				D_ASSERT(delete_data_file.partition_info.size() == data_file.partition_info.size());
				bool partition_matches = true;
				for (idx_t i = 0; i < delete_data_file.partition_info.size(); i++) {
					if (delete_data_file.partition_info[i] != data_file.partition_info[i]) {
						partition_matches = false;
						break;
					}
				}
				if (!partition_matches) {
					continue;
				}
			}
			result.emplace_back(delete_file);
		}
	}
	return result;
}

bool IcebergDeletePlanner::DeleteEntryMatchesFilters(const IcebergDeletePlanningContext &context,
                                                     const BoundIcebergManifestEntry &bound_manifest_entry) {
	auto manifest_idx = bound_manifest_entry.manifest_file_idx;
	if (!context.delete_manifest_matches[manifest_idx]) {
		return false;
	}
	if (!context.table_filters.HasFilters()) {
		return true;
	}
	return IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters)
	    .FileMatchesFilter(context.delete_manifests[manifest_idx].entry.file, bound_manifest_entry.entry);
}

unique_ptr<DeleteFilter> IcebergDeletePlanner::GetPositionalDeletesForFile(const IcebergDeletePlanningContext &context,
                                                                           const string &file_path) {
	auto &positional_delete_data = context.provider.PositionalDeleteData();
	auto it = positional_delete_data.find(file_path);
	return it == positional_delete_data.end() ? nullptr : it->second->ToFilter();
}

shared_ptr<IcebergDeleteData>
IcebergDeletePlanner::GetExistingPositionalDeleteData(const IcebergDeletePlanningContext &context,
                                                      const string &file_path) {
	auto &positional_delete_data = context.provider.PositionalDeleteData();
	auto it = positional_delete_data.find(file_path);
	return it == positional_delete_data.end() ? nullptr : it->second;
}

} // namespace duckdb
