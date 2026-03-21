#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct BoundIcebergManifestEntry {
public:
	BoundIcebergManifestEntry(idx_t file_idx, const IcebergManifestEntry &entry)
	    : manifest_file_idx(file_idx), entry(entry) {
	}

public:
	//! Reference to the IcebergManifestListEntry this entry originates from
	idx_t manifest_file_idx;
	const IcebergManifestEntry &entry;
};

} // namespace duckdb
