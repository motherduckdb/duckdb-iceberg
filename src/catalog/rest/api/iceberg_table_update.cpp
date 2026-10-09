#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static void AssignManifestFirstRowIds(const IcebergTableMetadata &metadata,
                                      optional_ptr<const IcebergSnapshot> current_snapshot,
                                      vector<IcebergManifestListEntry> &existing_manifest_list, int64_t &next_row_id) {
	if (metadata.iceberg_version < 3) {
		return;
	}
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.GetManifest();
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (manifest_file.first_row_id) {
			D_ASSERT(manifest_file.counts && manifest_file.counts->added_rows_count &&
			         manifest_file.counts->existing_rows_count);
			next_row_id =
			    MaxValue<int64_t>(next_row_id, *manifest_file.first_row_id + *manifest_file.counts->added_rows_count +
			                                       *manifest_file.counts->existing_rows_count);
			continue;
		}
		if (current_snapshot && current_snapshot->first_row_id) {
			throw InvalidConfigurationException(
			    "Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			    "entries have a 'first_row_id'");
		}
		D_ASSERT(manifest_file.counts && manifest_file.counts->added_rows_count &&
		         manifest_file.counts->existing_rows_count);
		manifest_file.first_row_id = next_row_id;
		next_row_id += *manifest_file.counts->added_rows_count;
		next_row_id += *manifest_file.counts->existing_rows_count;
	}
}

IcebergCommitState::IcebergCommitState(const IcebergTable &table_info, ClientContext &context)
    : table_info(table_info), context(context) {
	RefreshFromTable();
}

IcebergCommitState::~IcebergCommitState() = default;

const IcebergTableMetadata &IcebergCommitState::GetTableMetadata() const {
	return format_metadata ? *format_metadata : table_info.table_metadata;
}

void IcebergCommitState::RefreshFromTable() {
	format_metadata.reset();
	if (table_info.transaction_data) {
		auto &transaction_data = *table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			if (update->type != IcebergTableUpdateType::UPGRADE_FORMAT_VERSION) {
				continue;
			}
			// Replay local format changes where they occurred, rather than serializing
			// earlier snapshots with the final version already stored on table_info.
			format_metadata = make_uniq<IcebergTableMetadata>(table_info.table_metadata.Copy());
			format_metadata->iceberg_version = transaction_data.initial_format_version;
			if (format_metadata->iceberg_version < 3) {
				format_metadata->next_row_id.reset();
			}
			break;
		}
	}
	auto &metadata = GetTableMetadata();
	next_sequence_number = metadata.last_sequence_number + 1;
	next_row_id = 0;
	if (metadata.next_row_id) {
		next_row_id = *metadata.next_row_id;
	}
}

void IcebergCommitState::SetFormatVersion(int32_t format_version) {
	auto previous_version = GetTableMetadata().iceberg_version;
	if (format_version < previous_version) {
		throw InternalException("Cannot replay a format-version downgrade");
	}
	if (format_version == previous_version) {
		return;
	}
	if (!format_metadata) {
		format_metadata = make_uniq<IcebergTableMetadata>(table_info.table_metadata.Copy());
	}
	format_metadata->iceberg_version = format_version;
	if (previous_version < 3 && format_version >= 3) {
		next_row_id = 0;
		// This also includes V2 manifests written by earlier snapshots in this
		// attempt. Their first row IDs belong to the new V3 manifest list only.
		AssignManifestFirstRowIds(*format_metadata, latest_snapshot, manifests, next_row_id);
		format_metadata->next_row_id = next_row_id;
	}
}

void IcebergCommitState::LoadExistingManifests(DatabaseInstance &db,
                                               vector<IcebergManifestListEntry> &&existing_manifests) {
	manifests = std::move(existing_manifests);
	auto &metadata = GetTableMetadata();
	auto current_snapshot = metadata.GetLatestSnapshot();
	latest_snapshot = current_snapshot;
	if (manifests.empty() && current_snapshot) {
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = current_snapshot;
		snapshot_info.schema_id = metadata.GetCurrentSchemaId();

		IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, manifests);
	}

	//! In V1 the added/deleted/existing file counts were optional
	//! But even if the attributes are present, they're allowed to be NULL
	//! In both those situations we have to read all the entries of the manifest to materialize these counts on-demand
	for (auto &manifest : manifests) {
		auto &counts = manifest.GetFile().counts;
		if (counts && counts->Complete()) {
			continue;
		}
		manifest = IcebergManifestMerge::ScanManifestEntries(manifest, *this, metadata.GetCurrentSchemaId());
	}

	next_row_id = 0;
	if (metadata.next_row_id) {
		next_row_id = *metadata.next_row_id;
	}
	AssignManifestFirstRowIds(metadata, current_snapshot, manifests, next_row_id);
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type) : type(type) {
}

} // namespace duckdb
