#pragma once

#include "planning/deletes/iceberg_delete_planner.hpp"

namespace duckdb {

struct IcebergDeleteScanEntry {
	IcebergDeleteScanEntry(idx_t manifest_entry_index_p, BoundIcebergManifestEntry entry_p)
	    : manifest_entry_index(manifest_entry_index_p), entry(std::move(entry_p)) {
	}

	idx_t manifest_entry_index;
	BoundIcebergManifestEntry entry;
};

struct IcebergDeleteScanResult {
	position_delete_map_t positional_delete_data;
	equality_delete_map_t equality_delete_data;
};

struct IcebergDeleteFileScanner {
	static IcebergDeleteScanResult ScanFiles(const IcebergDeletePlanningContext &context,
	                                         const vector<IcebergDeleteScanEntry> &entries);
};

} // namespace duckdb
