#pragma once

#include "planning/deletes/iceberg_delete_planner.hpp"

namespace duckdb {

struct IcebergDeleteFileLoadState;

//! Input to ScanFiles, so it can run without holding the (delete_)lock
struct IcebergDeleteScanEntry {
	IcebergDeleteScanEntry(idx_t manifest_idx_p, idx_t entry_idx_p, const IcebergManifestListEntry &manifest_p,
	                       shared_ptr<IcebergDeleteFileLoadState> load_p)
	    : manifest_idx(manifest_idx_p), entry_idx(entry_idx_p), manifest(manifest_p), load(std::move(load_p)) {
	}

	const IcebergManifestEntry &GetEntry() const;
	BoundIcebergManifestEntry BindEntry() const;

	idx_t manifest_idx;
	idx_t entry_idx;
	const IcebergManifestListEntry &manifest;
	//! The shared LoadState of the delete file to populate
	shared_ptr<IcebergDeleteFileLoadState> load;
};

//! Intermediate to store the created delete file before adding it to the LoadState
struct IcebergEqualityDeleteScanResult {
	//! The LoadState to store the result into, after grabbing the lock
	shared_ptr<IcebergDeleteFileLoadState> load;
	//! The resulting equality delete data
	shared_ptr<IcebergEqualityDeleteFile> delete_file;
};

//! Grouped result of all delete files scanned for a data file
struct IcebergDeleteScanResult {
	position_delete_map_t positional_delete_data;
	vector<IcebergEqualityDeleteScanResult> equality_delete_data;
};

struct IcebergDeleteFileScanner {
	static IcebergDeleteScanResult ScanFiles(const IcebergDeletePlanningContext &context,
	                                         const vector<IcebergDeleteScanEntry> &entries);
};

} // namespace duckdb
