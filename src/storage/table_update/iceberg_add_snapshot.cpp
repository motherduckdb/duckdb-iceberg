#include "storage/table_update/iceberg_add_snapshot.hpp"

#include "../../include/metadata/iceberg_manifest_list.hpp"
#include "storage/irc_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
                                       IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_list(manifest_list_path),
      snapshot(std::move(snapshot)) {
}

rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject();
	return table_update;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	D_ASSERT(!manifest_list.manifest_entries.empty());

	auto manifest_files = manifest_list.GetManifestFiles();
	for (auto &manifest_file_ref : manifest_files) {
		auto manifest_file = manifest_file_ref.get();
		auto manifest_length =
		    manifest_file::WriteToFile(table_info, manifest_file.manifest_file, avro_copy, db, context);
		manifest_file.manifest_length = manifest_length;
	}

	manifest_list.manifest_entries.insert(manifest_list.manifest_entries.begin(),
	                                      std::make_move_iterator(commit_state.manifests.begin()),
	                                      std::make_move_iterator(commit_state.manifests.end()));
	manifest_list::WriteToFile(manifest_list, avro_copy, db, context);
	commit_state.manifests = std::move(manifest_list.manifest_entries);

	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(snapshot));
}

vector<reference<IcebergManifestListEntry>> IcebergManifestList::GetManifestFiles() {
	vector<reference<IcebergManifestListEntry>> ret;
	// TODO: Loop through every manfist list entry and then add the ManifestFile of the Manfist LIst
	return ret;
}

vector<reference<IcebergManifestListEntry>> IcebergManifestList::GetManifestFilesConst() const {
	vector<reference<IcebergManifestListEntry>> ret;
	// TODO: Loop through every manfist list entry and then add the ManifestFile of the Manfist LIst
	return ret;
}
} // namespace duckdb
