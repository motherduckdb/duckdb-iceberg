#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "avro_scan.hpp"
#include "iceberg_utils.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "manifest_reader.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergTable::IcebergTable(const IcebergSnapshot &snapshot_p) : snapshot(snapshot_p) {
}

unique_ptr<IcebergTable> IcebergTable::Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
                                            const IcebergSnapshot &snapshot, ClientContext &context,
                                            const IcebergOptions &options) {
	auto ret = make_uniq<IcebergTable>(snapshot);
	ret->path = iceberg_path;

	auto manifest_file_reader = make_uniq<manifest_file::ManifestFileReader>(metadata.iceberg_version, false);

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	IcebergManifestList manifest_list(manifest_list_full_path);

	//! Read the manifest list
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(metadata.iceberg_version);
	auto scan = AvroScan::ScanManifestList(snapshot, metadata, context, manifest_list_full_path);
	manifest_list_reader->Initialize(std::move(scan));
	auto &manifest_list_entries = manifest_list.GetManifestFilesMutable();
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read(STANDARD_VECTOR_SIZE, manifest_list_entries);
	}

	for (auto &manifest : manifest_list_entries) {
		auto scan =
		    AvroScan::ScanManifest(snapshot, manifest_list_entries, options, fs, iceberg_path, metadata, context);

		manifest_file_reader->Initialize(std::move(scan));
		// manifest_file_reader->SetSequenceNumber(manifest.sequence_number);
		// manifest_file_reader->SetPartitionSpecID(manifest.partition_spec_id);

		IcebergManifest manifest_file("???");
		while (!manifest_file_reader->Finished()) {
			manifest_file_reader->Read(STANDARD_VECTOR_SIZE, manifest_file.entries);
		}

		IcebergTableManifestEntry table_entry(std::move(manifest), std::move(manifest_file));
		ret->entries.push_back(table_entry);
	}
	return ret;
}

} // namespace duckdb
