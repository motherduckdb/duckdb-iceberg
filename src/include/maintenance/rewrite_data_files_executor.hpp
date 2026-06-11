#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct IcebergTableInformation;
class IcebergTableSchema;

struct RewriteExecutionResult {
	int64_t rewritten_data_files = 0;
	int64_t added_data_files = 0;
	int64_t rewritten_bytes = 0;
	//! Added files for the REPLACE snapshot.
	vector<IcebergManifestEntry> new_entries;
	//! Input files to remove in the same REPLACE snapshot.
	vector<RewriteCandidate> rewritten_candidates;
};

IcebergManifestEntry ExecuteOneGroup(ClientContext &context, const IcebergTableInformation &table_info,
                                     const MaintenanceTableKey &table_key,
                                     const vector<RewriteCandidate> &group, int64_t starting_sequence_number);

RewriteExecutionResult ExecuteRewrite(ClientContext &context, const RewritePlan &plan);

namespace rewrite_executor_internal {

//! Render the FIELD_IDS struct literal for the parquet COPY option.
string BuildFieldIdsLiteral(const IcebergTableSchema &schema);

//! Build the COPY SQL for one rewrite group.
string BuildRewriteCopySql(const string &catalog, const string &schema, const string &table,
                           const vector<string> &file_paths, const string &target_dir,
                           const string &field_ids_literal);

} // namespace rewrite_executor_internal

} // namespace duckdb
