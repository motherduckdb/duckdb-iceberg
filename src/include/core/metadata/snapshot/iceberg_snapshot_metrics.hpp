#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class IcebergSnapshot;
struct IcebergManifestListEntry;

//! Taken from https://iceberg.apache.org/spec/#metrics
enum class IcebergSnapshotMetricType : uint8_t {
	ADDED_DATA_FILES,   //! added-data-files - Number of data files added in the snapshot
	DELETED_DATA_FILES, //! deleted-data-files - Number of data files deleted in the snapshot
	TOTAL_DATA_FILES,   //! total-data-files - Total number of live data files in the snapshot
	ADDED_DELETE_FILES, //! added-delete-files - Number of positional/equality delete files and deletion vectors added
	                    //! in the snapshot
	ADDED_EQUALITY_DELETE_FILES, //! added-equality-delete-files - Number of equality delete files added in the snapshot
	REMOVED_EQUALITY_DELETE_FILES, //! removed-equality-delete-files - Number of equality delete files removed in the
	                               //! snapshot
	ADDED_POSITION_DELETE_FILES, //! added-position-delete-files - Number of position delete files added in the snapshot
	REMOVED_POSITION_DELETE_FILES, //! removed-position-delete-files - Number of position delete files removed in the
	                               //! snapshot
	ADDED_DVS,                     //! added-dvs - Number of deletion vectors added in the snapshot
	REMOVED_DVS,                   //! removed-dvs - Number of deletion vectors removed in the snapshot
	REMOVED_DELETE_FILES,   //! removed-delete-files - Number of positional/equality delete files and deletion vectors
	                        //! removed in the snapshot
	TOTAL_DELETE_FILES,     //! total-delete-files - Total number of live positional/equality delete files and deletion
	                        //! vectors in the snapshot
	ADDED_RECORDS,          //! added-records - Number of records added in the snapshot
	DELETED_RECORDS,        //! deleted-records - Number of records deleted in the snapshot
	TOTAL_RECORDS,          //! total-records - Total number of records in the snapshot
	ADDED_FILES_SIZE,       //! added-files-size - The size of files added in the snapshot
	REMOVED_FILES_SIZE,     //! removed-files-size - The size of files removed in the snapshot
	TOTAL_FILES_SIZE,       //! total-files-size - Total size of live files in the snapshot
	ADDED_POSITION_DELETES, //! added-position-deletes - Number of position delete records added in the snapshot
	REMOVED_POSITION_DELETES, //! removed-position-deletes - Number of position delete records removed in the snapshot
	TOTAL_POSITION_DELETES,   //! total-position-deletes - Total number of position delete records in the snapshot
	ADDED_EQUALITY_DELETES,   //! added-equality-deletes - Number of equality delete records added in the snapshot
	REMOVED_EQUALITY_DELETES, //! removed-equality-deletes - Number of equality delete records removed in the snapshot
	TOTAL_EQUALITY_DELETES,   //! total-equality-deletes - Total number of equality delete records in the snapshot
	DELETED_DUPLICATE_FILES,  //! deleted-duplicate-files - Number of duplicate files deleted (duplicates are files
	                          //! recorded more than once in the manifest)
	CHANGED_PARTITION_COUNT,  //! changed-partition-count - Number of partitions with files added or removed in the
	                          //! snapshot
	MANIFESTS_CREATED,        //! manifests-created - Number of manifest files created in the snapshot
	MANIFESTS_KEPT,           //! manifests-kept - Number of manifest files kept in the snapshot
	MANIFESTS_REPLACED,       //! manifests-replaced - Number of manifest files replaced in the snapshot
	ENTRIES_PROCESSED,        //! entries-processed - Number of manifest entries processed in the snapshot
};

using metrics_map_t = map<IcebergSnapshotMetricType, int64_t>;

struct IcebergSnapshotMetrics {
public:
	IcebergSnapshotMetrics();
	IcebergSnapshotMetrics(const IcebergSnapshot &parent_snapshot);

public:
	void AddManifestListEntry(const IcebergManifestListEntry &manifest_list_entry);
	void RemoveFileSize(int64_t file_size_in_bytes);
	bool HasTotalFilesSize() const;
	void SetTotalFilesSize(int64_t total_files_size);

private:
	void AddSizeMetric(IcebergSnapshotMetricType type, int64_t value);
	void UpdateTotalFilesSize(int64_t added, int64_t removed);
	void InheritMetric(const IcebergSnapshotMetrics &parent, IcebergSnapshotMetricType metric);
	static metrics_map_t EmptyMetrics();

private:
	metrics_map_t metrics;
};

} // namespace duckdb
