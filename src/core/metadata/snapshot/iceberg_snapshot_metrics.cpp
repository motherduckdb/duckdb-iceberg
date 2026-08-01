#include "core/metadata/snapshot/iceberg_snapshot_metrics.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

namespace {

struct IcebergSnapshotMetricItem {
	IcebergSnapshotMetricType type;
	const char *name;
};

static const IcebergSnapshotMetricItem SNAPSHOT_METRIC_KEYS[] = {
    {IcebergSnapshotMetricType::ADDED_DATA_FILES, "added-data-files"},
    {IcebergSnapshotMetricType::DELETED_DATA_FILES, "deleted-data-files"},
    {IcebergSnapshotMetricType::TOTAL_DATA_FILES, "total-data-files"},
    {IcebergSnapshotMetricType::ADDED_DELETE_FILES, "added-delete-files"},
    {IcebergSnapshotMetricType::ADDED_EQUALITY_DELETE_FILES, "added-equality-delete-files"},
    {IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETE_FILES, "removed-equality-delete-files"},
    {IcebergSnapshotMetricType::ADDED_POSITION_DELETE_FILES, "added-position-delete-files"},
    {IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, "removed-position-delete-files"},
    {IcebergSnapshotMetricType::ADDED_DVS, "added-dvs"},
    {IcebergSnapshotMetricType::REMOVED_DVS, "removed-dvs"},
    {IcebergSnapshotMetricType::REMOVED_DELETE_FILES, "removed-delete-files"},
    {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, "total-delete-files"},
    {IcebergSnapshotMetricType::ADDED_RECORDS, "added-records"},
    {IcebergSnapshotMetricType::DELETED_RECORDS, "deleted-records"},
    {IcebergSnapshotMetricType::TOTAL_RECORDS, "total-records"},
    {IcebergSnapshotMetricType::ADDED_FILES_SIZE, "added-files-size"},
    {IcebergSnapshotMetricType::REMOVED_FILES_SIZE, "removed-files-size"},
    {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, "total-files-size"},
    {IcebergSnapshotMetricType::ADDED_POSITION_DELETES, "added-position-deletes"},
    {IcebergSnapshotMetricType::REMOVED_POSITION_DELETES, "removed-position-deletes"},
    {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, "total-position-deletes"},
    {IcebergSnapshotMetricType::ADDED_EQUALITY_DELETES, "added-equality-deletes"},
    {IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETES, "removed-equality-deletes"},
    {IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, "total-equality-deletes"},
    {IcebergSnapshotMetricType::DELETED_DUPLICATE_FILES, "deleted-duplicate-files"},
    {IcebergSnapshotMetricType::CHANGED_PARTITION_COUNT, "changed-partition-count"},
    {IcebergSnapshotMetricType::MANIFESTS_CREATED, "manifests-created"},
    {IcebergSnapshotMetricType::MANIFESTS_KEPT, "manifests-kept"},
    {IcebergSnapshotMetricType::MANIFESTS_REPLACED, "manifests-replaced"},
    {IcebergSnapshotMetricType::ENTRIES_PROCESSED, "entries-processed"},
};

static const idx_t SNAPSHOT_METRIC_KEYS_SIZE = sizeof(SNAPSHOT_METRIC_KEYS) / sizeof(IcebergSnapshotMetricItem);

static string MetricsTypeToString(IcebergSnapshotMetricType type) {
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		if (item.type == type) {
			return item.name;
		}
	}
	throw InvalidConfigurationException("Metrics type not implemented: %d", static_cast<uint8_t>(type));
}

} // namespace

static IcebergSnapshotMetrics MetricsFromSummary(const case_insensitive_map_t<string> &snapshot_summary) {
	IcebergSnapshotMetrics ret;
	auto &metrics = ret.metrics;
	//! Remove the default for `TOTAL_FILES_SIZE`, leaving it uninitialized if not present in the summary, rather than
	//! setting it to 0.
	metrics.erase(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	bool file_size_metrics_are_valid = true;
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		const bool is_file_size_metric = item.type == IcebergSnapshotMetricType::ADDED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::REMOVED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::TOTAL_FILES_SIZE;
		auto it = snapshot_summary.find(item.name);
		if (it == snapshot_summary.end()) {
			//! Not present in the summary
			continue;
		}
		auto raw_value = it->second;
		int64_t value;
		try {
			value = std::stoll(it->second);
		} catch (...) {
			if (is_file_size_metric) {
				file_size_metrics_are_valid = false;
			}
			// Skip invalid metrics
			continue;
		}
		if (is_file_size_metric && value < 0) {
			file_size_metrics_are_valid = false;
			continue;
		}
		metrics[item.type] = value;
	}
	if (!file_size_metrics_are_valid) {
		metrics.erase(IcebergSnapshotMetricType::ADDED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::REMOVED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	}
	return ret;
}

static metrics_map_t IcebergSnapshotMetrics::EmptyMetrics() {
	//! First snapshot of a table, start all totals at 0
	return metrics_map_t({{IcebergSnapshotMetricType::TOTAL_DATA_FILES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_RECORDS, 0},
	                      {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, 0},
	                      {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, 0}});
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics() : metrics(EmptyMetrics()) {
}

void IcebergSnapshotMetrics::InheritMetric(const IcebergSnapshotMetrics &parent, IcebergSnapshotMetricType metric) {
	auto it = parent.metrics.find(metric);
	if (it == parent.metrics.end()) {
		//! Nothing to inherit
		return;
	}
	metrics[metric] = it->second;
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics(const IcebergSnapshot &parent_snapshot) {
	//! Start metrics from a parent snapshot, inherit all totals
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_RECORDS);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_POSITION_DELETES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES);
}

void IcebergSnapshotMetrics::AddSizeMetric(IcebergSnapshotMetricType type, int64_t value) {
	auto &metric = metrics.emplace(type, 0).first->second;
	metric = IcebergUtils::AddFileSizeChecked(metric, value);
}

void IcebergSnapshotMetrics::UpdateTotalFilesSize(int64_t added, int64_t removed) {
	auto total_it = metrics.find(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	if (total_it == metrics.end()) {
		return;
	}
	int64_t with_added;
	int64_t updated;
	if (!TryAddOperator::Operation(total_it->second, added, with_added) ||
	    !TrySubtractOperator::Operation(with_added, removed, updated)) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' exceeds the supported BIGINT range");
	}
	if (updated < 0) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' cannot be negative");
	}
	total_it->second = updated;
}

void IcebergSnapshotMetrics::AddManifestListEntry(const IcebergManifestListEntry &manifest_list_entry) {
	if (!manifest_list_entry.metrics) {
		throw InternalException("New manifest was produced without metrics!?");
	}
	auto &manifest_metrics = *manifest_list_entry.metrics;

	if (manifest_metrics.added_files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::ADDED_FILES_SIZE, manifest_metrics.added_files_size);
	}
	if (manifest_metrics.removed_files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, manifest_metrics.removed_files_size);
	}
	UpdateTotalFilesSize(manifest_metrics.added_files_size, manifest_metrics.removed_files_size);

	auto &manifest_file = manifest_list_entry.file;
	if (manifest_file.content == IcebergManifestContentType::DELETE) {
		//! Delete file count metrics
		metrics.emplace(IcebergSnapshotMetricType::ADDED_DELETE_FILES, 0).first->second +=
		    manifest_metrics.added_delete_files;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_DELETE_FILES, 0).first->second +=
		    manifest_metrics.removed_delete_files;
		{
			auto it = metrics.find(IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
			if (it != metrics.end()) {
				auto previous = it->second;
				int64_t total_delete_files =
				    previous + manifest_metrics.added_delete_files - manifest_metrics.removed_delete_files;
				if (total_delete_files >= 0) {
					metrics[IcebergSnapshotMetricType::TOTAL_DELETE_FILES] = total_delete_files;
				}
			}
		}

		//! Deletion records metrics
		metrics.emplace(IcebergSnapshotMetricType::ADDED_POSITION_DELETES, 0).first->second +=
		    manifest_metrics.added_position_deletes;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, 0).first->second +=
		    manifest_metrics.added_position_delete_files;

		metrics.emplace(IcebergSnapshotMetricType::ADDED_DVS, 0).first->second +=
		    manifest_metrics.added_deletion_vectors;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_DVS, 0).first->second +=
		    manifest_metrics.removed_deletion_vectors;

		//! Total for position delete files (positional-deletes and dvs)
		{
			auto it = metrics.find(IcebergSnapshotMetricType::TOTAL_POSITION_DELETES);
			if (it != metrics.end()) {
				auto previous = it->second;
				int64_t new_total = previous + manifest_metrics.added_position_delete_files -
				                    manifest_metrics.removed_position_delete_files;
				if (new_total >= 0) {
					metrics[IcebergSnapshotMetricType::TOTAL_POSITION_DELETES] = new_total;
				}
			}
		}

		metrics.emplace(IcebergSnapshotMetricType::ADDED_EQUALITY_DELETES, 0).first->second +=
		    manifest_metrics.added_equality_deletes;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETE_FILES, 0).first->second +=
		    manifest_metrics.added_equality_delete_files;
		//! Total for equality delete files
		{
			auto it = metrics.find(IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES);
			if (it != metrics.end()) {
				auto previous = it->second;
				int64_t new_total = previous + manifest_metrics.added_equality_delete_files -
				                    manifest_metrics.removed_equality_delete_files;
				if (new_total >= 0) {
					metrics[IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES] = new_total;
				}
			}
		}
		return;
	}

	//! Data file count metrics
	metrics.emplace(IcebergSnapshotMetricType::ADDED_DATA_FILES, 0).first->second += manifest_metrics.added_data_files;
	metrics.emplace(IcebergSnapshotMetricType::DELETED_DATA_FILES, 0).first->second +=
	    manifest_metrics.deleted_data_files;
	{
		auto it = metrics.find(IcebergSnapshotMetricType::TOTAL_DATA_FILES);
		if (it != metrics.end()) {
			auto previous = it->second;
			int64_t new_total = previous + manifest_metrics.added_data_files - manifest_metrics.deleted_data_files;
			if (new_total >= 0) {
				metrics[IcebergSnapshotMetricType::TOTAL_DATA_FILES] = new_total;
			}
		}
	}

	//! Data record metrics
	metrics.emplace(IcebergSnapshotMetricType::ADDED_RECORDS, 0).first->second += manifest_metrics.added_records;
	metrics.emplace(IcebergSnapshotMetricType::DELETED_RECORDS, 0).first->second += manifest_metrics.deleted_records;
	{
		auto it = metrics.find(IcebergSnapshotMetricType::TOTAL_RECORDS);
		if (it != metrics.end()) {
			auto previous = it->second;
			int64_t new_total = previous + manifest_metrics.added_records - manifest_metrics.deleted_records;
			if (new_total >= 0) {
				metrics[IcebergSnapshotMetricType::TOTAL_RECORDS] = new_total;
			}
		}
	}
}

void IcebergSnapshotMetrics::RemoveFileSize(int64_t file_size_in_bytes) {
	AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, file_size_in_bytes);
	UpdateTotalFilesSize(0, file_size_in_bytes);
}

bool IcebergSnapshotMetrics::HasTotalFilesSize() const {
	return metrics.count(IcebergSnapshotMetricType::TOTAL_FILES_SIZE) != 0;
}

void IcebergSnapshotMetrics::SetTotalFilesSize(int64_t total_files_size) {
	if (total_files_size < 0) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' cannot be negative");
	}
	metrics[IcebergSnapshotMetricType::TOTAL_FILES_SIZE] = total_files_size;
}

} // namespace duckdb
