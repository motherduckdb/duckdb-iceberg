#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct IcebergEqualityDeleteFile {
public:
	explicit IcebergEqualityDeleteFile(idx_t manifest_entry_index_p) : manifest_entry_index(manifest_entry_index_p) {
	}
	IcebergEqualityDeleteFile(const IcebergEqualityDeleteFile &) = delete;
	IcebergEqualityDeleteFile &operator=(const IcebergEqualityDeleteFile &) = delete;

public:
	//! Index in IcebergScanPlanProvider::DeleteManifestEntries(). Unlike a reference, this remains stable when lazy
	//! manifest enumeration grows the provider's vector.
	idx_t manifest_entry_index;
	//! Columns follow the referenced manifest entry's data_file.equality_ids order.
	DataChunk equality_values;
};

} // namespace duckdb
