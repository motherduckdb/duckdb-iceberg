#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

namespace duckdb {

struct IcebergFilePruner {
public:
	IcebergFilePruner(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergTableSchema &schema,
	                  const IcebergTableFilters &table_filters)
	    : context(context), metadata(metadata), schema(schema), table_filters(table_filters) {
	}

	bool ManifestMatchesFilter(const IcebergManifestFile &manifest) const;
	bool FileMatchesFilter(const IcebergManifestFile &manifest_file, const IcebergManifestEntry &manifest_entry) const;

private:
	bool FilePartitionMatchesFilter(const IcebergDataFile &data_file, const IcebergManifestFile &manifest_file) const;

private:
	ClientContext &context;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	const IcebergTableFilters &table_filters;
};

} // namespace duckdb
