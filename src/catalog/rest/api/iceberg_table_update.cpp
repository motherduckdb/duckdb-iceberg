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
                                      vector<IcebergManifestListEntry> &existing_manifest_list,
                                      IcebergRowIdAllocator &row_ids) {
	if (metadata.iceberg_version < 3) {
		return;
	}
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.GetManifest();
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (!manifest_file.first_row_id && current_snapshot && current_snapshot->first_row_id) {
			throw InvalidConfigurationException(
			    "Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			    "entries have a 'first_row_id'");
		}
		row_ids.AssignExistingManifest(manifest_file);
	}
}

IcebergCommitState::IcebergCommitState(const IcebergTable &table_info, ClientContext &context)
    : table_info(table_info), next_sequence_number(table_info.table_metadata.last_sequence_number + 1),
      row_ids(table_info.table_metadata.iceberg_version >= 3 ? table_info.table_metadata.next_row_id.value_or(0) : 0),
      context(context) {
}

void IcebergCommitState::LoadExistingManifests(DatabaseInstance &db,
                                               vector<IcebergManifestListEntry> &&existing_manifests) {
	manifests = std::move(existing_manifests);
	auto current_snapshot = table_info.table_metadata.GetLatestSnapshot();
	latest_snapshot = current_snapshot;
	if (manifests.empty() && current_snapshot) {
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = current_snapshot;
		snapshot_info.schema_id = table_info.table_metadata.GetCurrentSchemaId();

		IcebergManifestList::LoadManifestFiles(snapshot_info, table_info.table_metadata, context, manifests);
	}

	//! In V1 the added/deleted/existing file counts were optional
	//! But even if the attributes are present, they're allowed to be NULL
	//! In both those situations we have to read all the entries of the manifest to materialize these counts on-demand
	for (auto &manifest : manifests) {
		auto &counts = manifest.GetFile().counts;
		if (counts && counts->Complete()) {
			continue;
		}
		manifest =
		    IcebergManifestMerge::ScanManifestEntries(manifest, *this, table_info.table_metadata.GetCurrentSchemaId());
	}

	AssignManifestFirstRowIds(table_info.table_metadata, current_snapshot, manifests, row_ids);
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type) : type(type) {
}

} // namespace duckdb
