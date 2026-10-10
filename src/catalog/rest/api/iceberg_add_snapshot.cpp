#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"
#include "catalog/rest/iceberg_table_set.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTable &table_info, IcebergSnapshotOperationType operation)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT), operation(operation) {
	//! FIXME: Do we also need to capture the current partition spec and sort order?
	//! This is a bit of a code smell, the `IcebergTable` should instead be const
	//! and all transactional changes should live in the IcebergTransactionData
	schema_id = table_info.table_metadata.GetCurrentSchemaId();
}

bool IcebergAddSnapshot::IsRetryable() const {
	//! DELETE-retry safety is enforced in StageSingleTableCommit.
	return operation == IcebergSnapshotOperationType::APPEND || operation == IcebergSnapshotOperationType::DELETE;
}

static optional<IcebergManifestListEntry> RewriteManifestFile(const IcebergManifestListEntry &list_entry,
                                                              IcebergSnapshotWriter &writer,
                                                              IcebergCommitState &commit_state, int32_t schema_id,
                                                              const VersionedIcebergManifestDeletes &deletes) {
	auto loaded_manifest = list_entry.HasManifestEntries()
	                           ? list_entry
	                           : IcebergManifestMerge::ScanManifestEntries(list_entry, commit_state, schema_id);
	D_ASSERT(loaded_manifest.manifest_metadata);
	const auto &file = loaded_manifest.GetFile();

	auto rewritten_entries = file.PrepareEntriesForRewrite(std::move(loaded_manifest.GetManifestEntries()));
	bool removed_any_entries = false;
	for (auto &manifest_entry : rewritten_entries) {
		if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED &&
		    deletes.IsInvalidated({manifest_entry.data_file.file_path, manifest_entry.data_file.content_offset})) {
			writer.RemoveManifestEntry(manifest_entry);
			manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
			//! Inherits this snapshot, which deleted the file; conflict checks in other engines rely on it
			manifest_entry.SetSnapshotId(nullopt);
			removed_any_entries = true;
		}
	}
	if (!removed_any_entries) {
		return nullopt;
	}
	return writer.WriteReplacementManifest(*loaded_manifest.manifest_metadata, std::move(rewritten_entries),
	                                       file.first_row_id);
}

void IcebergAddSnapshot::ConstructManifestList(IcebergSnapshotWriter &writer, IcebergCommitState &commit_state) const {
	for (auto &manifest : commit_state.manifests) {
		if (manifest_deletes) {
			auto replacement = RewriteManifestFile(manifest, writer, commit_state, schema_id, *manifest_deletes);
			if (replacement) {
				writer.AddExistingManifest(std::move(*replacement));
				continue;
			}
		}
		writer.AddExistingManifest(std::move(manifest));
	}
	commit_state.manifests.clear();
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto writer = commit_state.CreateSnapshotWriter(schema_id, operation);
	ConstructManifestList(writer, commit_state);
	for (const auto &manifest : pending_manifests) {
		writer.WriteManifest(manifest);
	}
	commit_state.AddWrittenSnapshot(IcebergWrittenSnapshot::Create(std::move(writer)));
}

void IcebergAddSnapshot::AddPendingManifest(IcebergPendingManifest manifest) {
	pending_manifests.push_back(std::move(manifest));
}

void IcebergAddSnapshot::SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes_p) {
	manifest_deletes.emplace(std::move(manifest_deletes_p));
}

const vector<IcebergPendingManifest> &IcebergAddSnapshot::GetPendingManifests() const {
	return pending_manifests;
}

} // namespace duckdb
