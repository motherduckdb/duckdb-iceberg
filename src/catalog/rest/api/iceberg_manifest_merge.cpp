#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/map.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config
//===--------------------------------------------------------------------===//
namespace {

constexpr const char *MERGE_ENABLED = "commit.manifest-merge.enabled";
constexpr const char *MIN_COUNT_TO_MERGE = "commit.manifest.min-count-to-merge";
constexpr const char *TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";

//! Aligned with Apache Iceberg Java defaults (the authoritative reference).
constexpr bool MERGE_ENABLED_DEFAULT = true;
constexpr idx_t MIN_COUNT_TO_MERGE_DEFAULT = 100;
constexpr int64_t TARGET_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024;

bool ParseBoolProperty(const string &value, bool fallback) {
	if (value.empty()) {
		return fallback;
	}
	auto lowered = StringUtil::Lower(value);
	if (lowered == "true") {
		return true;
	}
	if (lowered == "false") {
		return false;
	}
	return fallback;
}

template <class T>
T ParseIntProperty(const string &value, T fallback) {
	if (value.empty()) {
		return fallback;
	}
	try {
		auto parsed = std::stoll(value);
		//! A non-positive threshold is meaningless; fall back rather than disable merging silently.
		return parsed > 0 ? static_cast<T>(parsed) : fallback;
	} catch (...) {
		return fallback;
	}
}

//! Resolve the schema id a manifest was written under, so that only manifests sharing a schema are
//! ever merged. The schema id drives the data_file.partition layout and column stats, so mixing
//! schemas can drop or misalign stats.
//!
//! A manifest added by THIS transaction was written under the table's current schema, so we use
//! that directly (its Avro key-value metadata is not materialized here anyway). A carried-over
//! manifest carries its schema id in its Avro header key-value metadata (populated once the file is
//! opened -- see the pre-load in MergeManifests): prefer the "schema-id" key, else parse the
//! "schema" JSON's "schema-id" (the reference implementations populate only "schema", not
//! "schema-id"). The metadata is required by the spec, so its absence is an error (see below).
int32_t ResolveManifestSchemaId(const MergeInputManifest &input, int32_t current_schema_id) {
	using namespace duckdb_yyjson;

	if (input.source == ManifestSource::NEW_THIS_TRANSACTION) {
		return current_schema_id;
	}

	auto &metadata = input.entry.metadata;

	auto id_it = metadata.find("schema-id");
	if (id_it != metadata.end() && !id_it->second.empty()) {
		try {
			return static_cast<int32_t>(std::stoi(id_it->second));
		} catch (...) {
			//! Malformed; fall through to the schema JSON.
		}
	}

	auto schema_it = metadata.find("schema");
	if (schema_it != metadata.end() && !schema_it->second.empty()) {
		const string &schema_json = schema_it->second;
		auto doc =
		    std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(schema_json.c_str(), schema_json.size(), 0));
		if (doc) {
			auto root = yyjson_doc_get_root(doc.get());
			if (root) {
				auto schema_id_val = yyjson_obj_get(root, "schema-id");
				if (schema_id_val && yyjson_is_int(schema_id_val)) {
					return static_cast<int32_t>(yyjson_get_int(schema_id_val));
				}
			}
		}
	}

	//! The Iceberg spec requires manifests to carry their schema in the Avro key-value metadata (all
	//! format versions), and DuckDB always writes it (see the backfill in manifest_file::WriteToFile).
	//! Reaching here means a carried-over manifest is missing it, which we cannot merge safely, so we
	//! surface it as a configuration error rather than guessing a schema id. InvalidConfiguration (not
	//! Internal) so it does not invalidate the database/session.
	throw InvalidConfigurationException(
	    "Cannot merge manifest '%s': it is missing the required schema id in its Avro key-value metadata",
	    input.entry.file.manifest_path);
}

} // namespace

ManifestMergeConfig ManifestMergeConfig::FromTableMetadata(const IcebergTableMetadata &metadata) {
	ManifestMergeConfig config;
	config.enabled = ParseBoolProperty(metadata.GetTableProperty(MERGE_ENABLED), MERGE_ENABLED_DEFAULT);
	config.min_count_to_merge =
	    ParseIntProperty<idx_t>(metadata.GetTableProperty(MIN_COUNT_TO_MERGE), MIN_COUNT_TO_MERGE_DEFAULT);
	config.target_size_bytes =
	    ParseIntProperty<int64_t>(metadata.GetTableProperty(TARGET_SIZE_BYTES), TARGET_SIZE_BYTES_DEFAULT);
	return config;
}

//===--------------------------------------------------------------------===//
// Bin-packing (pure logic)
//===--------------------------------------------------------------------===//
vector<vector<idx_t>> BinPackManifests(const vector<int64_t> &weights, int64_t target_weight) {
	//! Mirror Java `ManifestMergeManager`/PyIceberg `ListPacker.pack_end` exactly: first-fit
	//! bin-packing with lookback=1 over the reversed input, then reverse the result. lookback=1 keeps
	//! at most one open bin, so manifests are not reordered across distant positions -- this is what
	//! makes the under-filled bin land on the newest manifests (merged next time) and avoids random
	//! deletes when data files are later aged off (see Java mergeGroup comment).
	//!
	//! PackingIterator semantics: maintain a list of open bins; place an item into the first open bin
	//! that fits, else open a new bin; whenever the number of open bins exceeds `lookback`, close
	//! (emit) the oldest open bin (FIFO). At the end, emit the remaining open bins in order.
	constexpr idx_t LOOKBACK = 1;

	struct OpenBin {
		vector<idx_t> items;
		int64_t weight = 0;
	};

	//! Pack over the reversed input (pack_end).
	vector<vector<idx_t>> packed; // closed bins, in close order
	vector<OpenBin> open_bins;
	for (idx_t rev = weights.size(); rev-- > 0;) {
		auto weight = weights[rev];
		OpenBin *target = nullptr;
		for (auto &b : open_bins) {
			if (b.weight + weight <= target_weight) {
				target = &b;
				break;
			}
		}
		if (target) {
			target->items.push_back(rev);
			target->weight += weight;
		} else {
			OpenBin b;
			b.items.push_back(rev);
			b.weight = weight;
			open_bins.push_back(std::move(b));
			if (open_bins.size() > LOOKBACK) {
				//! Close the oldest open bin (FIFO).
				packed.push_back(std::move(open_bins.front().items));
				open_bins.erase(open_bins.begin());
			}
		}
	}
	for (auto &b : open_bins) {
		packed.push_back(std::move(b.items));
	}

	//! pack_end: reverse each bin's items and the list of bins to restore original ascending order.
	vector<vector<idx_t>> result;
	result.reserve(packed.size());
	for (idx_t i = packed.size(); i-- > 0;) {
		auto &bin = packed[i];
		std::reverse(bin.begin(), bin.end());
		result.push_back(std::move(bin));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Merge decision
//===--------------------------------------------------------------------===//
bool ShouldMergeBin(const vector<idx_t> &bin, idx_t min_count_to_merge) {
	if (bin.size() <= 1) {
		return false;
	}
	//! Only merge a bin once it has accumulated at least `min-count-to-merge` manifests (Apache
	//! Iceberg's ManifestMergeManager semantics). A first-fit bin can under-fill (its manifests do
	//! not reach the target size), and such a bin must not be rewritten until enough small manifests
	//! pile up -- otherwise every commit rewrites a handful of manifests, churning metadata and
	//! changing the manifest layout that read-time pruning relies on.
	if (bin.size() < min_count_to_merge) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Merge execution
//===--------------------------------------------------------------------===//

//! Read the manifest_entries of a manifest from its Avro file, reusing the vectorized manifest
//! reader. Returns the list entry with `manifest_entries` populated. Shared by the delete-rewrite
//! path and the merge path so both load entries identically.
IcebergManifestListEntry ScanManifestEntries(const IcebergManifestListEntry &list_entry,
                                             IcebergCommitState &commit_state, int32_t schema_id) {
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(list_entry);

	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = commit_state.latest_snapshot;
	snapshot_info.schema_id = schema_id;

	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot_info, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);
	while (!reader->Finished()) {
		reader->Read();
	}
	return std::move(manifest_files[0]);
}

namespace {

//! Merge one spec-homogeneous bin into a single new manifest. Returns the new list entry.
IcebergManifestListEntry MergeBin(const vector<MergeInputManifest> &input, const vector<idx_t> &bin,
                                  IcebergManifestContentType content, CopyFunction &avro_copy, DatabaseInstance &db,
                                  IcebergCommitState &commit_state, int32_t schema_id, int32_t partition_spec_id,
                                  int64_t snapshot_id) {
	auto &table_metadata = commit_state.table_info.table_metadata;
	const bool is_v3 = table_metadata.iceberg_version >= 3;

	//! Gather every entry from the bin's manifests, preserving status and historical sequence
	//! numbers. New (ADDED) entries keep inherited sequence numbers; everything else keeps its
	//! materialized historical value.
	vector<IcebergManifestEntry> merged_entries;
	//! Merging is a pure physical repack: it creates no new rows, so V3 row lineage must be
	//! preserved, not reassigned. The first_row_id is a manifest-file-level value, so the merged
	//! manifest's first_row_id is the smallest first_row_id among the manifests it absorbs.
	//! This uses file-level metadata only -- no entry read required. Carried-over manifests always
	//! have a first_row_id by this point: a V2->V3 upgraded snapshot assigns one to every existing
	//! DATA manifest earlier in the commit (see IcebergTransactionData's upgrade handling), and new
	//! V3 data manifests are excluded from merging (they inherit their id only at write time).
	bool has_first_row_id = false;
	int64_t min_first_row_id = 0;
	//! The merged manifest's min_sequence_number must be the smallest data_sequence_number among its
	//! entries. CreateFromEntries cannot compute this (it derives it from the manifest-file sequence
	//! number, which is a placeholder here), so we track the true minimum and set it ourselves before
	//! the manifest is written / handed to AddNewManifestFile. If left as the placeholder, scan
	//! planning's `seq > X` pruning would mis-judge which historical data the manifest can contain.
	bool has_min_seq = false;
	int64_t min_seq = 0;
	for (auto idx : bin) {
		auto &member = input[idx].entry;
		const bool carried_over = input[idx].source == ManifestSource::CARRIED_OVER;
		if (is_v3 && member.file.first_row_id.has_value()) {
			if (!has_first_row_id || *member.file.first_row_id < min_first_row_id) {
				min_first_row_id = *member.file.first_row_id;
				has_first_row_id = true;
			}
		}
		auto loaded = member.manifest_entries.empty() ? ScanManifestEntries(member, commit_state, schema_id) : member;
		//! V3 row lineage: a data file's _row_id is derived from its data_file.first_row_id (+ row
		//! position). That id is normally left null on disk and inherited at read time from the
		//! manifest's first_row_id plus the record_count of preceding files that also lack one. Merging
		//! rewrites these entries into a NEW manifest whose first_row_id is the min of the sources, so
		//! the inherited ids would be re-derived against that new base. The Iceberg spec requires the
		//! inherited value to be materialized into the data file metadata when creating existing/deleted
		//! entries, so we compute each entry's first_row_id here -- against THIS source manifest's
		//! first_row_id -- before the entries are moved into the merged manifest. That makes the merged
		//! row ids independent of the merged layout. (DELETE manifests carry no first_row_id, so this
		//! only applies to DATA content.)
		//!
		//! Note: with today's ordering invariants (carried-over manifests are prepended in first_row_id
		//! order and are contiguous), reading back would recompute the same ids even without this step;
		//! materializing is spec-compliant and keeps correctness independent of those invariants.
		if (is_v3 && content == IcebergManifestContentType::DATA && member.file.first_row_id.has_value()) {
			int64_t inherited_row_id = *member.file.first_row_id;
			for (auto &entry : loaded.manifest_entries) {
				if (!entry.data_file.HasFirstRowId()) {
					entry.data_file.SetFirstRowId(inherited_row_id);
					inherited_row_id += entry.data_file.record_count;
				}
			}
		}
		for (auto &entry : loaded.manifest_entries) {
			//! DELETED entries: only a drop made by THIS snapshot is carried into the new manifest; a
			//! dropped entry from an earlier snapshot is discarded (it already took effect and would
			//! only bloat the merged manifest forever). Mirrors Java ManifestMergeManager.createManifest.
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				if (entry.HasSnapshotId() && entry.GetSnapshotId() == snapshot_id) {
					merged_entries.push_back(std::move(entry));
				}
				continue;
			}
			//! Entries absorbed from an already-committed manifest are EXISTING in the new snapshot:
			//! demote their ADDED status (the file was added by an earlier snapshot, not this one) so
			//! the merged manifest's added/existing counts and the snapshot summary stay correct.
			//! Their historical sequence numbers are preserved by GetSequenceNumber.
			//! Entries from this transaction's own new manifest keep ADDED (genuinely new this commit).
			if (carried_over && entry.status == IcebergManifestEntryStatusType::ADDED) {
				auto seq = entry.GetSequenceNumber(member.file);
				auto file_seq = entry.GetFileSequenceNumber(member.file);
				entry.SetSequenceNumber(seq);
				entry.SetFileSequenceNumber(file_seq);
				entry.status = IcebergManifestEntryStatusType::EXISTING;
			}
			//! Live entries determine the manifest's minimum data sequence number.
			auto entry_seq = entry.GetSequenceNumber(member.file);
			if (!has_min_seq || entry_seq < min_seq) {
				min_seq = entry_seq;
				has_min_seq = true;
			}
			merged_entries.push_back(std::move(entry));
		}
	}

	//! A bin can collapse to nothing (e.g. every entry was a DELETED-by-an-earlier-snapshot entry and
	//! was filtered out above). An empty manifest must never be written -- WriteToFile asserts on it
	//! (and an empty Avro manifest is meaningless). Return an entry with no manifest_entries so the
	//! caller drops it; do this BEFORE CreateFromEntries/WriteToFile.
	if (merged_entries.empty()) {
		IcebergManifestListEntry empty {IcebergManifestFile {""}};
		return empty;
	}

	//! CreateFromEntries computes all counts, min_sequence_number and the partition field summary
	//! from the entries; pass the bin's own spec id so the summary is computed against the correct
	//! (possibly historical) spec, not the table default. We pass a throwaway row-id
	//! counter so the global one is not advanced, then restore the lineage-preserving value below.
	int64_t scratch_row_id = 0;
	auto result =
	    IcebergManifestListEntry::CreateFromEntries(FileSystem::GetFileSystem(commit_state.context),
	                                                /*snapshot_id*/ -1, /*sequence_number*/ 0, table_metadata, content,
	                                                std::move(merged_entries), scratch_row_id, partition_spec_id);
	//! first_row_id applies to DATA manifests only; a DELETE manifest's first_row_id is always null.
	if (is_v3 && content == IcebergManifestContentType::DATA && has_first_row_id) {
		result.file.first_row_id = min_first_row_id;
	} else {
		result.file.first_row_id.reset();
	}
	//! Set the true minimum data sequence number from the absorbed entries (see above). Done before
	//! WriteToFile / AddNewManifestFile so scan-planning pruning sees the correct lower bound.
	if (has_min_seq) {
		result.file.min_sequence_number = min_seq;
	}

	auto manifest_length = manifest_file::WriteToFile(table_metadata, result.file, result.manifest_entries, avro_copy,
	                                                  db, commit_state.context, &result.metadata);
	result.file.manifest_length = manifest_length;
	return result;
}

} // namespace

vector<IcebergManifestListEntry> MergeManifests(vector<MergeInputManifest> &&input, IcebergManifestContentType content,
                                                const ManifestMergeConfig &config, CopyFunction &avro_copy,
                                                DatabaseInstance &db, IcebergCommitState &commit_state,
                                                int32_t current_schema_id, int64_t snapshot_id) {
	vector<IcebergManifestListEntry> result;
	if (!config.enabled || input.size() <= 1) {
		for (auto &member : input) {
			result.push_back(std::move(member.entry));
		}
		return result;
	}

	//! Group by (schema id, partition spec id): only manifests sharing BOTH may be merged together.
	//! The schema id governs the data_file.partition layout and column statistics, so merging across
	//! schemas could drop or misalign stats; the partition spec id governs the partition tuple
	//! itself. Grouping is ordered (map) so the output manifest order is deterministic regardless of
	//! input order.
	//!
	//! Carried-over manifests reach this point via the manifest LIST read only, so their Avro header
	//! metadata (which holds the schema id) is not materialized yet. Open each one here -- reading
	//! its entries at the same time -- so grouping can read the real schema id and MergeBin can reuse
	//! the already-loaded entries instead of opening the file again. Manifests added by this
	//! transaction never carry this metadata, but they also do not need it: their schema id is the
	//! current schema id (resolved via ManifestSource in ResolveManifestSchemaId).
	for (auto &member : input) {
		if (member.entry.manifest_entries.empty()) {
			member.entry = ScanManifestEntries(member.entry, commit_state, current_schema_id);
		}
	}

	map<std::pair<int32_t, int32_t>, vector<idx_t>> groups;
	for (idx_t i = 0; i < input.size(); i++) {
		auto schema_id = ResolveManifestSchemaId(input[i], current_schema_id);
		auto spec_id = input[i].entry.file.partition_spec_id;
		groups[std::make_pair(schema_id, spec_id)].push_back(i);
	}

	for (auto &group : groups) {
		auto schema_id = group.first.first;
		auto spec_id = group.first.second;
		auto &group_indices = group.second;

		//! Bin-pack using manifest-file-level lengths only. Indices in `bins` are into
		//! `group_indices`.
		vector<int64_t> weights;
		weights.reserve(group_indices.size());
		for (auto idx : group_indices) {
			weights.push_back(input[idx].entry.file.manifest_length);
		}
		auto bins = BinPackManifests(weights, config.target_size_bytes);

		for (auto &local_bin : bins) {
			//! Translate local (group) indices back to global input indices.
			vector<idx_t> bin;
			bin.reserve(local_bin.size());
			for (auto local : local_bin) {
				bin.push_back(group_indices[local]);
			}

			if (!ShouldMergeBin(bin, config.min_count_to_merge)) {
				for (auto idx : bin) {
					result.push_back(std::move(input[idx].entry));
				}
				continue;
			}

			auto merged = MergeBin(input, bin, content, avro_copy, db, commit_state, schema_id, spec_id, snapshot_id);
			//! A bin can collapse to nothing (e.g. all entries were deleted and filtered out); never
			//! write or reference an empty manifest.
			if (merged.manifest_entries.empty()) {
				continue;
			}
			result.push_back(std::move(merged));
		}
	}
	return result;
}

} // namespace duckdb
