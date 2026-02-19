#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "storage/catalog/iceberg_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
                                       IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_list(manifest_list_path),
      snapshot(std::move(snapshot)) {
}

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTableInformation &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info);
	return table_update;
}

void IcebergAddSnapshot::ConstructManifest(IcebergCommitState &commit_state, const IcebergManifestFile &manifest_file,
                                           IcebergManifestDeletes &deletes) {
	vector<IcebergManifestFile> manifest_files = {manifest_file};
	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;
	auto &snapshot = *table_metadata.GetLatestSnapshot();
	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan, true);
	vector<IcebergManifestEntry> manifest_entries;
	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read(STANDARD_VECTOR_SIZE, manifest_entries);
	}

	bool removed_any_entries = false;
	idx_t handled_entries = 0;
	for (auto it = manifest_entries.begin(); it != manifest_entries.end();) {
		auto &manifest_entry = *it;
		auto delete_it = deletes.altered_data_files.find(manifest_entry.data_file.file_path);
		if (delete_it == deletes.altered_data_files.end()) {
			//! Include the entry, it wasn't invalidated
			++it;
			continue;
		}
		handled_entries++;
		auto &delete_file_state = delete_it->second;
		if (delete_file_state.type != IcebergDeleteType::DELETION_VECTOR) {
			//! This is a positional delete,
			//! we can't remove the file because we can't be sure
			//! that it doesn't contain deletes for other data files.
			++it;
			continue;
		}
		removed_any_entries = true;
		D_ASSERT(delete_file_state.referenced_data_files.size() == 1);
		D_ASSERT(manifest_entry.data_file.referenced_data_file == delete_file_state.referenced_data_files[0]);
		//! Remove the entry
		it = manifest_entries.erase(it);
	}
	if (handled_entries != deletes.altered_data_files.size()) {
		throw InternalException("(ConstructManifest) We expected to find %d invalidated entries, but only found %d",
		                        deletes.altered_data_files.size(), handled_entries);
	}
	if (!removed_any_entries) {
		return;
	}
	throw InternalException("now what?");
}

void IcebergAddSnapshot::ConstructManifestList(IcebergCommitState &commit_state) {
	if (altered_manifests.empty()) {
		//! Copy the existing manifest_file entries without any modification
		manifest_list.AddToManifestEntries(commit_state.manifests);
		return;
	}

	case_insensitive_map_t<idx_t> manifest_file_path_to_idx;
	for (idx_t i = 0; i < commit_state.manifests.size(); i++) {
		auto &manifest_file = commit_state.manifests[i];
		manifest_file_path_to_idx.emplace(manifest_file.manifest_path, i);
	}

	for (auto &entry : altered_manifests) {
		auto it = manifest_file_path_to_idx.find(entry.first);
		if (it == manifest_file_path_to_idx.end()) {
			throw InternalException("Can't find path '%s' in ConstructManifestList", entry.first);
		}
		auto &manifest_file = commit_state.manifests[it->second];
		auto &altered_manifest = entry.second;

		ConstructManifest(commit_state, manifest_file, altered_manifest);
	}
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	D_ASSERT(manifest_list.GetManifestListEntriesCount() != 0);
	//! TODO: keep transaction-state for the existing manifests,
	//! to monitor for "dirty" manifests that need to be rewritten.

	//! Write the avro files for the new manifests
	auto manifest_list_entries_size = manifest_list.GetManifestListEntriesCount();
	for (idx_t manifest_index = 0; manifest_index < manifest_list_entries_size; manifest_index++) {
		manifest_list.WriteManifestListEntry(table_info, manifest_index, avro_copy, db, context);
	}

	//! Add manifest_file entries from the previous snapshot (if any)
	ConstructManifestList(commit_state);
	manifest_list::WriteToFile(table_info.table_metadata, manifest_list, avro_copy, db, context);
	commit_state.manifests = manifest_list.GetManifestListEntries();

	//! Finally add a Iceberg REST Catalog 'TableUpdate' to commit
	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(table_info, snapshot));
}

} // namespace duckdb
