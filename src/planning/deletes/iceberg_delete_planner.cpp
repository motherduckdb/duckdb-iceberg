#include "planning/deletes/iceberg_delete_planner.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

vector<idx_t>
IcebergDeletePlanner::GetDeleteManifestsForDataFile(const IcebergDeletePlanningContext &context,
                                                    const BoundIcebergManifestEntry &data_manifest_entry) {
	vector<idx_t> result;
	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	auto file_pruner = IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters);
	for (idx_t manifest_idx = 0; manifest_idx < context.delete_manifests.size(); manifest_idx++) {
		if (!context.delete_manifest_matches[manifest_idx]) {
			continue;
		}
		auto &delete_manifest = context.delete_manifests[manifest_idx].entry.file;
		if (!file_pruner.DeleteManifestMatchesDataFile(delete_manifest, data_manifest, data_manifest_entry.entry)) {
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
	auto file_pruner = IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters);
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
			auto &delete_manifest = context.delete_manifests[delete_manifest_entry.manifest_file_idx].entry.file;
			if (!file_pruner.DeleteFileMatchesDataFile(delete_manifest, delete_manifest_entry.entry, manifest_file,
			                                           manifest_entry)) {
				continue;
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

bool IcebergDeletePlanner::DeleteEntryAppliesToDataFile(const IcebergDeletePlanningContext &context,
                                                        const BoundIcebergManifestEntry &delete_manifest_entry,
                                                        const BoundIcebergManifestEntry &data_manifest_entry) {
	auto &delete_file = delete_manifest_entry.entry.data_file;
	auto &data_file = data_manifest_entry.entry.data_file;
	if (!context.provider.DeleteFileAppliesToDataFile(data_file.file_path, delete_file.file_path)) {
		return false;
	}

	auto &delete_manifest = context.delete_manifests[delete_manifest_entry.manifest_file_idx].entry.file;
	auto &data_manifest = context.data_manifests[data_manifest_entry.manifest_file_idx].entry.file;
	return IcebergFilePruner(context.context, context.metadata, context.schema, context.table_filters)
	    .DeleteFileMatchesDataFile(delete_manifest, delete_manifest_entry.entry, data_manifest,
	                               data_manifest_entry.entry);
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
