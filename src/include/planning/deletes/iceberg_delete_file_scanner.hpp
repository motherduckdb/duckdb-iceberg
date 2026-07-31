#pragma once

#include "planning/deletes/iceberg_delete_planner.hpp"

namespace duckdb {

struct IcebergDeleteFileLoadState;

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
	shared_ptr<IcebergDeleteFileLoadState> load;
};

struct IcebergEqualityDeleteScanResult {
	shared_ptr<IcebergDeleteFileLoadState> load;
	shared_ptr<IcebergEqualityDeleteFile> delete_file;
};

struct IcebergDeleteScanResult {
	position_delete_map_t positional_delete_data;
	vector<IcebergEqualityDeleteScanResult> equality_delete_data;
};

struct IcebergDeleteFileScanner {
	static IcebergDeleteScanResult ScanFiles(const IcebergDeletePlanningContext &context,
	                                         const vector<IcebergDeleteScanEntry> &entries);
};

} // namespace duckdb
