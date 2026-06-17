#include "maintenance/rewrite_data_files_planner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

namespace {

void GroupCandidates(RewritePlan &plan, int64_t target_file_size_bytes, int64_t min_input_files, bool rewrite_all) {
	if (plan.candidates.empty()) {
		return;
	}

	std::map<string, vector<RewriteCandidate>> per_partition;
	for (auto &cand : plan.candidates) {
		if (!rewrite_all && cand.file_size_in_bytes >= target_file_size_bytes) {
			continue;
		}
		per_partition[rewrite_planner_internal::PartitionBucketKey(cand.partition_info)].push_back(cand);
	}

	for (auto &kv : per_partition) {
		if (!rewrite_all && static_cast<int64_t>(kv.second.size()) < min_input_files) {
			continue;
		}
		plan.file_groups.push_back(std::move(kv.second));
	}
}

} // namespace

namespace rewrite_planner_internal {

string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info) {
	if (partition_info.empty()) {
		return "";
	}
	vector<IcebergPartitionInfo> sorted = partition_info;
	std::sort(sorted.begin(), sorted.end(),
	          [](const IcebergPartitionInfo &a, const IcebergPartitionInfo &b) { return a.field_id < b.field_id; });
	//! Length-prefix values so partition keys cannot collide on delimiters.
	string out;
	for (auto &p : sorted) {
		out += std::to_string(p.field_id);
		out += ":";
		if (p.value.IsNull()) {
			out += "NULL";
		} else {
			auto val_str = p.value.ToString();
			out += std::to_string(val_str.size());
			out += ":";
			out += val_str;
		}
		out += ";";
	}
	return out;
}

} // namespace rewrite_planner_internal

RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input) {
	RewritePlan plan;
	plan.table_name = input.table_name;
	plan.target_file_size_bytes = input.target_file_size_bytes;

	auto table_info_ptr = ReloadIcebergTableShared(context, input.table_name, "iceberg_rewrite_data_files");
	auto &table_info = *table_info_ptr;
	auto &table_metadata = table_info.table_metadata;
	plan.table_info = std::move(table_info_ptr);

	if (table_metadata.iceberg_version >= 3) {
		throw NotImplementedException("iceberg_rewrite_data_files does not yet support V3 tables");
	}

	auto latest_snapshot = table_metadata.GetLatestSnapshot();
	if (!latest_snapshot) {
		//! Brand-new table — no snapshot to rewrite against. Caller treats this
		//! as a no-op success; executor and commit step must not run on this plan.
		plan.table_is_empty = true;
		return plan;
	}

	plan.starting_snapshot_id = latest_snapshot->snapshot_id;
	//! Rewritten files use the sequence number of the snapshot being compacted.
	plan.starting_sequence_number = latest_snapshot->sequence_number;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = latest_snapshot;
	snapshot_info.schema_id = table_metadata.GetCurrentSchemaId();

	IcebergOptions options;
	auto manifest_list =
	    IcebergManifestList::Load(table_metadata.GetLocation(), table_metadata, snapshot_info, context, options);

	const auto &manifest_files = manifest_list->GetManifestFilesConst();
	if (manifest_files.empty()) {
		plan.table_is_empty = true;
		return plan;
	}

	auto default_spec_id = table_metadata.default_spec_id;

	for (idx_t mi = 0; mi < manifest_files.size(); ++mi) {
		const auto &list_entry = manifest_files[mi];
		if (list_entry.file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		//! Guard against partition spec evolution: if a manifest was written
		//! under a different partition spec, its partition tuples may not match
		//! the current default spec. Mixing specs in one rewrite group would
		//! produce incorrect manifest metadata. Reject until multi-spec support
		//! is implemented.
		if (list_entry.file.partition_spec_id != default_spec_id) {
			throw NotImplementedException(
			    "iceberg_rewrite_data_files: table has data files written under partition spec %d "
			    "but current default spec is %d; partition spec evolution is not yet supported",
			    list_entry.file.partition_spec_id, default_spec_id);
		}

		const auto &entries = list_entry.manifest_entries;
		for (idx_t ei = 0; ei < entries.size(); ++ei) {
			const auto &entry = entries[ei];
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			if (entry.data_file.content != IcebergManifestEntryContentType::DATA) {
				continue;
			}

			RewriteCandidate cand;
			cand.file_path = entry.data_file.file_path;
			cand.file_size_in_bytes = entry.data_file.file_size_in_bytes;
			cand.record_count = entry.data_file.record_count;
			cand.partition_info = entry.data_file.partition_info;
			cand.partition_spec_id = list_entry.file.partition_spec_id;
			cand.manifest_idx = mi;
			cand.entry_idx = ei;
			plan.candidates.push_back(std::move(cand));
		}
	}

	if (plan.candidates.empty()) {
		plan.table_is_empty = true;
		return plan;
	}

	GroupCandidates(plan, input.target_file_size_bytes, input.min_input_files, input.rewrite_all);

	if (plan.file_groups.empty()) {
		plan.table_is_empty = true;
	}
	return plan;
}

} // namespace duckdb
