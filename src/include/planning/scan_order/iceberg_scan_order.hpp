#pragma once

#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

class IcebergTableSchema;
struct RowGroupOrderOptions;

class IcebergScanOrder {
public:
	IcebergScanOrder();
	~IcebergScanOrder();

	void Set(unique_ptr<RowGroupOrderOptions> options);
	unique_ptr<RowGroupOrderOptions> CopyOptions() const;
	optional_ptr<const RowGroupOrderOptions> GetOptions() const;
	bool IsPending() const;

	void Apply(ClientContext &context, const IcebergTableSchema &schema, bool has_matching_delete_manifests,
	           vector<BoundIcebergManifestEntry> &manifest_entries);

private:
	unique_ptr<RowGroupOrderOptions> options;
	bool applied = false;
};

} // namespace duckdb
