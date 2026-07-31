#pragma once

#include "core/deletes/iceberg_delete_data.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

namespace duckdb {

class FileSystem;
class IcebergScanPlanProvider;
struct IcebergTableMetadata;
class IcebergTableSchema;
struct IcebergFilePruner;
struct IcebergOptions;

using equality_delete_map_t = map<sequence_number_t, vector<unique_ptr<IcebergEqualityDeleteFile>>>;
using position_delete_map_t = unordered_map<string, shared_ptr<IcebergDeleteData>>;

struct IcebergDeletePlanningContext {
	ClientContext &context;
	FileSystem &fs;
	const string &table_path;
	const IcebergOptions &options;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	const IcebergTableFilters &table_filters;
	const vector<BoundIcebergManifestListEntry> &data_manifests;
	const vector<BoundIcebergManifestListEntry> &delete_manifests;
	const vector<bool> &delete_manifest_matches;
	IcebergScanPlanProvider &provider;
};

struct IcebergDeletePlanner {
	static vector<idx_t> GetDeleteManifestsForDataFile(const IcebergDeletePlanningContext &context,
	                                                   const BoundIcebergManifestEntry &data_manifest_entry);
	static vector<reference<const IcebergEqualityDeleteFile>>
	GetEqualityDeletesForFile(const IcebergDeletePlanningContext &context,
	                          const BoundIcebergManifestEntry &data_manifest_entry);
	static bool DeleteEntryMatchesFilters(const IcebergDeletePlanningContext &context,
	                                      const BoundIcebergManifestEntry &delete_manifest_entry);
	static bool DeleteEntryAppliesToDataFile(const IcebergDeletePlanningContext &context,
	                                         const BoundIcebergManifestEntry &delete_manifest_entry,
	                                         const BoundIcebergManifestEntry &data_manifest_entry);
	static unique_ptr<DeleteFilter> GetPositionalDeletesForFile(const IcebergDeletePlanningContext &context,
	                                                            const string &file_path);
	static shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const IcebergDeletePlanningContext &context,
	                                                                     const string &file_path);
};

} // namespace duckdb
