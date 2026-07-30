//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/column_index_map.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/deletes/iceberg_delete_data.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

using equality_delete_map_t = map<sequence_number_t, vector<IcebergEqualityDeleteFile>>;
using position_delete_map_t = unordered_map<string, shared_ptr<IcebergDeleteData>>;

struct IcebergTableFilters {
	using filter_set_t = column_index_map<unique_ptr<ExpressionFilter>>;
	using iterator = filter_set_t::iterator;
	using const_iterator = filter_set_t::const_iterator;

public:
	bool HasFilters() const {
		return !table_filters.empty();
	}
	idx_t FilterCount() const {
		return table_filters.size();
	}
	void PushFilter(const ColumnIndex &column_idx, unique_ptr<ExpressionFilter> table_filter) {
		D_ASSERT(table_filters.find(column_idx) == table_filters.end());
		table_filters[column_idx] = std::move(table_filter);
	}
	optional_ptr<const ExpressionFilter> TryGetFilterByColumnIndex(const ColumnIndex &column_idx) const {
		auto entry = table_filters.find(column_idx);
		if (entry == table_filters.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	iterator begin() { // NOLINT: match stl API
		return table_filters.begin();
	}
	iterator end() { // NOLINT: match stl API
		return table_filters.end();
	}
	const_iterator begin() const { // NOLINT: match stl API
		return table_filters.begin();
	}
	const_iterator end() const { // NOLINT: match stl API
		return table_filters.end();
	}

private:
	filter_set_t table_filters;
};

class IcebergTableEntry;
class IcebergScanPlanProvider;
class ClientSideScanPlanProvider;
struct IcebergDeleteManifestLoadState;
struct IcebergMultiFileList;
struct IcebergMultiFileReader;
struct RowGroupOrderOptions;

struct IcebergManifestScanningState {
public:
	IcebergManifestScanningState(ClientContext &context, unique_ptr<AvroScan> scan,
	                             vector<IcebergManifestListEntry> &list_entries)
	    : context(context), executor(context), scan(std::move(scan)), list_entries(list_entries), in_progress_tasks(0) {
	}

public:
	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	vector<IcebergManifestListEntry> &list_entries;
	atomic<idx_t> in_progress_tasks;
};

struct IcebergMultiFileListSharedState {
public:
	IcebergMultiFileListSharedState(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, string path,
	                                const IcebergOptions &options);
	~IcebergMultiFileListSharedState();

private:
	friend struct IcebergMultiFileList;
	friend class ClientSideScanPlanProvider;

	ClientContext &context;
	FileSystem &fs;
	shared_ptr<IcebergScanInfo> scan_info;
	string path;
	optional_ptr<IcebergTableEntry> table;
	IcebergOptions options;

	mutable annotated_mutex lock;
	mutable annotated_mutex delete_lock DUCKDB_ACQUIRED_AFTER(lock);
	mutable ManifestEntryReadState read_state;

	mutable bool server_side_planning_enabled DUCKDB_GUARDED_BY(lock) = true;

	mutable bool manifest_list_loaded DUCKDB_GUARDED_BY(lock) = false;
	mutable bool data_manifest_scan_started DUCKDB_GUARDED_BY(lock) = false;

	//! Scanned delete manifests and their owners.
	mutable vector<IcebergManifestListEntry> committed_delete_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<reference<const IcebergManifestListEntry>> transaction_delete_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<shared_ptr<IcebergDeleteManifestLoadState>> delete_manifest_loads DUCKDB_GUARDED_BY(delete_lock);
	mutable vector<bool> delete_manifest_entries_enumerated DUCKDB_GUARDED_BY(delete_lock);
	mutable idx_t next_delete_entry_to_process DUCKDB_GUARDED_BY(delete_lock) = 0;
	mutable vector<BoundIcebergManifestEntry> delete_manifest_entries DUCKDB_GUARDED_BY(delete_lock);

	//! Scanned data manifests and their owners.
	mutable vector<IcebergManifestListEntry> committed_data_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<reference<const IcebergManifestListEntry>> transaction_data_manifests DUCKDB_GUARDED_BY(lock);
	mutable unique_ptr<IcebergManifestScanningState> data_manifest_read_state DUCKDB_GUARDED_BY(lock);

	//! Declared after the manifest owners so references in parsed delete data are destroyed first.
	mutable position_delete_map_t positional_delete_data DUCKDB_GUARDED_BY(delete_lock);
	mutable equality_delete_map_t equality_delete_data DUCKDB_GUARDED_BY(delete_lock);

	//! Populated as parsed data-file entries become visible to any filtered view.
	mutable unordered_map<string, vector<IcebergPartitionInfo>> data_file_partition_info DUCKDB_GUARDED_BY(lock);
};

struct IcebergDataViewCursor {
public:
	idx_t next_batch_idx = 0;
	bool has_current_batch = false;
	ManifestReadBatch current_batch;
	idx_t current_batch_offset = 0;
};

struct IcebergMultiFileList : public MultiFileList {
private:
	friend struct IcebergMultiFileReader;

public:
	IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                     const IcebergOptions &options);
	virtual ~IcebergMultiFileList() override;

public:
	//! MultiFileList API
	unique_ptr<MultiFileList> DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	OpenFileInfo GetFile(idx_t i) const override;

public:
	void SetTable(IcebergTableEntry &table);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;
	vector<IcebergPartitionInfo> GetPartitionInfoForDataFile(const string &file_path) const;
	void SetScanOrder(unique_ptr<RowGroupOrderOptions> options);
	optional_ptr<IcebergTableEntry> GetTable() const;
	void DisableServerSidePlanning();

private:
	string GetPath() const;
	const IcebergTableMetadata &GetMetadata() const;
	const IcebergTransactionData &GetTransactionData() const;
	const IcebergSnapshotScanInfo &GetSnapshot() const;
	const IcebergTableSchema &GetSchema() const;

	void SetOptions(const IcebergOptions &options);
	void Bind(vector<LogicalType> &return_types, vector<Identifier> &names);
	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters,
	                                                  const vector<ColumnIndex> &column_indexes) const;
	IcebergMultiFileList(shared_ptr<IcebergMultiFileListSharedState> shared_state);
	void GetStatistics(vector<PartitionStatistics> &result) const;

	void InitializeView(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);

	bool HasTransactionData() const;
	//! Reorder (and prune, when a LIMIT is present) the materialized data files by the
	//! ORDER BY column's per-file min/max bounds, mirroring the native RowGroupReorderer.
	void EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	OpenFileInfo GetFileInternal(idx_t i, annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);
	BoundIcebergManifestEntry GetManifestEntry(idx_t file_id) const;
	IcebergManifestFile GetManifestFileForDataFile(idx_t file_id) const;
	const IcebergManifestFile &GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
	                                                   IcebergManifestContentType type) const
	    DUCKDB_REQUIRES(shared_state->lock);

	void ProcessDeletes(const BoundIcebergManifestEntry &data_manifest_entry) const;
	unique_ptr<DeleteFilter> GetPositionalDeletesForFile(const string &file_path) const;
	vector<reference<const IcebergEqualityDeleteFile>>
	GetEqualityDeletesForFile(const BoundIcebergManifestEntry &manifest_entry) const;

	bool ManifestMatchesFilter(const IcebergManifestFile &manifest) const;
	bool DeleteManifestMatchesDataFile(const IcebergManifestFile &delete_manifest,
	                                   const BoundIcebergManifestEntry &data_manifest_entry) const
	    DUCKDB_REQUIRES(shared_state->lock);
	vector<idx_t> GetDeleteManifestsForDataFile(const BoundIcebergManifestEntry &data_manifest_entry) const
	    DUCKDB_REQUIRES(shared_state->lock);
	bool FilePartitionMatchesFilter(const IcebergDataFile &data_file, const IcebergManifestFile &manifest_file,
	                                const IcebergTableMetadata &metadata, const IcebergTableSchema &schema) const;
	bool FileMatchesFilter(const IcebergManifestFile &manifest_file, const IcebergManifestEntry &manifest_entry,
	                       IcebergManifestContentType file_type) const;
	//! Whether a delete file's manifest entry can apply to any file selected by the current scan filter.
	//! Delete files are pruned on partition only: one whose partition is excluded by the filter cannot
	//! delete a row from any surviving data file, so it does not need to be read.
	bool DeleteEntryMatchesFilters(const BoundIcebergManifestEntry &bound_manifest_entry) const
	    DUCKDB_REQUIRES(shared_state->lock);

	//! NOTE: this requires the lock because it modifies the 'data_files' vector, potentially invalidating references
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id,
	                                                          annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);

	unique_ptr<ExpressionFilter> GetFilterForColumnIndex(const ColumnIndex &column_index) const;

	bool TryGetNextBatch(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void FinishScanTasks(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void LoadManifestList(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void InitializeScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	void StartDataManifestScan(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void EnumerateDeleteManifestEntriesInternal(const vector<idx_t> &manifest_indexes) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ProcessDeletesInternal(const vector<idx_t> &manifest_indexes) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ScanDeleteFiles() const DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ScanParquetDeleteFiles(const vector<reference<const BoundIcebergManifestEntry>> &entries,
	                            IcebergManifestEntryContentType content) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ScanPositionalDeleteFile(const BoundIcebergManifestEntry &manifest_entry, DataChunk &result) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ScanEqualityDeleteFile(const BoundIcebergManifestEntry &manifest_entry, DataChunk &result,
	                            const vector<MultiFileColumnDefinition> &columns,
	                            const vector<string> &source_names) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	void ScanPuffinFile(const BoundIcebergManifestEntry &entry) const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	position_delete_map_t &GetPositionalDeleteData() const
	    DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	equality_delete_map_t &GetEqualityDeleteData() const DUCKDB_REQUIRES(shared_state->lock, shared_state->delete_lock);
	IcebergScanPlanProvider &GetScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);

private:
	friend class ClientSideScanPlanProvider;
	friend class ServerSideScanPlanProvider;

	shared_ptr<IcebergMultiFileListSharedState> shared_state;
	ClientContext &context;
	FileSystem &fs;
	const IcebergOptions &options;
	//! ComplexFilterPushdown results
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	IcebergTableFilters table_filters;

	//! The provider is per-view. The server-side implementation owns its filter-derived plan, while the client-side
	//! implementation delegates to the shared manifest state above.
	mutable unique_ptr<IcebergScanPlanProvider> scan_plan_provider DUCKDB_GUARDED_BY(shared_state->lock);

	//! Combination of committed + transaction delete manifests
	mutable vector<BoundIcebergManifestListEntry> delete_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> delete_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);
	//! Conservative until InitializeView determines whether this filtered view has any matching delete manifests.
	mutable atomic<bool> has_matching_delete_manifests {true};

	mutable IcebergDataViewCursor data_view_cursor DUCKDB_GUARDED_BY(shared_state->lock);
	//! References to items inside the 'manifest_entries' of the list entries in the 'data_manifests'
	mutable vector<BoundIcebergManifestEntry> data_manifest_entries DUCKDB_GUARDED_BY(shared_state->lock);
	//! Combination of committed + transaction data manifests
	mutable vector<BoundIcebergManifestListEntry> data_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> data_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);

	//! Set by the table function's set_scan_order callback when an ORDER BY ... LIMIT can drive scan order.
	mutable unique_ptr<RowGroupOrderOptions> scan_order_options DUCKDB_GUARDED_BY(shared_state->lock);
	mutable bool scan_order_applied DUCKDB_GUARDED_BY(shared_state->lock) = false;
};

} // namespace duckdb
