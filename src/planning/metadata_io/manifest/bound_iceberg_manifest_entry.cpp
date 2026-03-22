#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

void BoundIcebergManifestEntry::SetFirstRowId(int64_t value) {
	has_first_row_id = true;
	first_row_id = value;
}

int64_t BoundIcebergManifestEntry::GetFirstRowId() const {
	D_ASSERT(has_first_row_id);
	return first_row_id;
}

bool BoundIcebergManifestEntry::HasFirstRowId() const {
	return has_first_row_id;
}

} // namespace duckdb
