#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

ServerSideScanPlanProvider::ServerSideScanPlanProvider(IcebergServerSideScanPlan plan_p) : plan(std::move(plan_p)) {
	delete_manifest_entries_enumerated.resize(plan.delete_manifests.size(), false);
}

void ServerSideScanPlanProvider::LoadManifestList() {
}

void ServerSideScanPlanProvider::ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count) {
}

void ServerSideScanPlanProvider::StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) {
	if (data_manifest_scan_started) {
		return;
	}
	data_manifest_scan_started = true;
	for (idx_t i = 0; i < plan.data_manifests.size(); i++) {
		read_state.PushBatch(ManifestReadBatch {i, 0, plan.data_manifests[i].GetManifestEntries().size()});
	}
}

void ServerSideScanPlanProvider::EnumerateDeleteManifestEntries(const vector<idx_t> &manifest_indexes) {
	for (auto manifest_idx : manifest_indexes) {
		if (manifest_idx >= plan.delete_manifests.size()) {
			throw InternalException("Selected server-side delete manifest index %llu is out of bounds", manifest_idx);
		}
		if (delete_manifest_entries_enumerated[manifest_idx]) {
			continue;
		}
		auto &manifest_list_entry = plan.delete_manifests[manifest_idx];
		auto manifest = BoundIcebergManifestListEntry(manifest_idx, manifest_list_entry);
		for (auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
			if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED) {
				delete_manifest_entries.push_back(manifest.BindEntry(manifest_entry));
			}
		}
		delete_manifest_entries_enumerated[manifest_idx] = true;
	}
}

bool ServerSideScanPlanProvider::TryGetNextBatch(IcebergDataViewCursor &cursor) {
	return cursor.has_current_batch || read_state.TryReadBatch(cursor);
}

void ServerSideScanPlanProvider::FinishScanTasks() {
}

bool ServerSideScanPlanProvider::DeleteFileAppliesToDataFile(const string &data_file_path,
                                                             const string &delete_file_path) const {
	auto refs = plan.delete_files_by_data_file.find(data_file_path);
	return refs != plan.delete_files_by_data_file.end() && refs->second.count(delete_file_path);
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DataManifests() {
	return plan.data_manifests;
}

vector<IcebergManifestListEntry> &ServerSideScanPlanProvider::DeleteManifests() {
	return plan.delete_manifests;
}

idx_t &ServerSideScanPlanProvider::NextDeleteEntryToProcess() {
	return next_delete_entry_to_process;
}

vector<BoundIcebergManifestEntry> &ServerSideScanPlanProvider::DeleteManifestEntries() {
	return delete_manifest_entries;
}

position_delete_map_t &ServerSideScanPlanProvider::PositionalDeleteData() {
	return positional_delete_data;
}

equality_delete_map_t &ServerSideScanPlanProvider::EqualityDeleteData() {
	return equality_delete_data;
}

} // namespace duckdb
