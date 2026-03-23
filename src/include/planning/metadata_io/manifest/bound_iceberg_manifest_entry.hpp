#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct BoundIcebergManifestEntry {
public:
	BoundIcebergManifestEntry(idx_t file_idx, const IcebergManifestEntry &entry)
	    : manifest_file_idx(file_idx), entry(entry) {
	}

public:
	void SetFirstRowId(int64_t first_row_id);
	int64_t GetFirstRowId() const;
	bool HasFirstRowId() const;

public:
	//! Reference to the IcebergManifestListEntry this entry originates from
	idx_t manifest_file_idx;
	const IcebergManifestEntry &entry;

private:
	//! The materialized first row id of the data file
	bool has_first_row_id = false;
	int64_t first_row_id;
};

} // namespace duckdb
