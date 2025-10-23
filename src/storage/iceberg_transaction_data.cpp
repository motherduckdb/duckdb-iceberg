#include "storage/iceberg_transaction_data.hpp"

#include "../include/metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_update/common.hpp"
#include "storage/iceberg_table_information.hpp"

#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static int64_t NewSnapshotId() {
	auto random_number = UUID::GenerateRandomUUID().upper;
	if (random_number < 0) {
		// Flip the sign bit using XOR with 1LL shifted left 63 bits
		random_number ^= (1LL << 63);
	}
	return random_number;
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files) {
	D_ASSERT(!data_files.empty());

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = NewSnapshotId();

	auto last_sequence_number = table_metadata.last_sequence_number;
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto sequence_number = last_sequence_number + 1;

	//! Construct the manifest file
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_metadata.GetMetadataPath() + "/" + manifest_file_uuid + "-m0.avro";
	IcebergManifestFile new_manifest_file(manifest_file_path);

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.operation = operation;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	new_snapshot.has_parent_snapshot = table_info.table_metadata.has_current_snapshot || !alters.empty();
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			new_snapshot.parent_snapshot_id = last_alter.snapshot.snapshot_id;
		} else {
			D_ASSERT(table_info.table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
		}
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));

	IcebergManifestListEntry &manifest_list_entry = add_snapshot->manifest_list.CreateNewManifestListEntry();
	auto manifest_file = manifest_list_entry.manifest_file;

	// auto &manifest = add_snapshot->manifest;
	auto &snapshot = add_snapshot->snapshot;

	manifest_list_entry.manifest_path = manifest_file_path;
	manifest_list_entry.sequence_number = sequence_number;
	if (operation == IcebergSnapshotOperationType::APPEND) {
		manifest_list_entry.content = IcebergManifestContentType::DATA;
	} else if (operation == IcebergSnapshotOperationType::DELETE) {
		manifest_list_entry.content = IcebergManifestContentType::DELETE;
	}
	manifest_list_entry.added_files_count = data_files.size();
	manifest_list_entry.deleted_files_count = 0;
	manifest_list_entry.existing_files_count = 0;
	manifest_list_entry.added_rows_count = 0;
	manifest_list_entry.existing_rows_count = 0;
	//! TODO: support partitions
	manifest_list_entry.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	//! Add the data files
	for (auto &data_file : data_files) {
		if (operation == IcebergSnapshotOperationType::APPEND) {
			manifest_list_entry.added_rows_count += data_file.record_count;
		} else if (operation == IcebergSnapshotOperationType::DELETE) {
			manifest_list_entry.deleted_rows_count += data_file.record_count;
		}
		data_file.sequence_number = snapshot.sequence_number;
		data_file.snapshot_id = snapshot.snapshot_id;
		data_file.partition_spec_id = manifest_list_entry.partition_spec_id;
		if (!manifest_list_entry.has_min_sequence_number ||
		    data_file.sequence_number < manifest_list_entry.min_sequence_number) {
			manifest_list_entry.min_sequence_number = data_file.sequence_number;
		}
		manifest_list_entry.has_min_sequence_number = true;
	}
	manifest_list_entry.added_snapshot_id = snapshot.snapshot_id;
	manifest_file.data_files.insert(manifest_file.data_files.end(), std::make_move_iterator(data_files.begin()),
	                                std::make_move_iterator(data_files.end()));
	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddUpdateSnapshot(unordered_map<string, IcebergDeleteFileInfo> &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files) {
	// //! Generate a new snapshot id
	// auto &table_metadata = table_info.table_metadata;
	// auto snapshot_id = NewSnapshotId();
	//
	// auto last_sequence_number = table_metadata.last_sequence_number;
	// if (!alters.empty()) {
	// 	auto &last_alter = alters.back().get();
	// 	last_sequence_number = last_alter.snapshot.sequence_number;
	// }
	//
	// auto sequence_number = last_sequence_number + 1;
	//
	// //! Construct the manifest list
	// auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	// auto manifest_list_path =
	// 	table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";
	//
	// //! Construct the manifest file for the delete data
	// auto delete_manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	// auto delete_manifest_file_path = table_metadata.GetMetadataPath() + "/" + delete_manifest_file_uuid + "-m0.avro";
	// IcebergManifestFile delete_manifest_file_tmp(delete_manifest_file_path);
	//
	// //! Construct the snapshot
	// IcebergSnapshot new_snapshot;
	// new_snapshot.operation = IcebergSnapshotOperationType::OVERWRITE;
	// new_snapshot.snapshot_id = snapshot_id;
	// new_snapshot.sequence_number = sequence_number;
	// new_snapshot.schema_id = table_metadata.current_schema_id;
	// new_snapshot.manifest_list = manifest_list_path;
	// new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	//
	// new_snapshot.has_parent_snapshot = table_info.table_metadata.has_current_snapshot || !alters.empty();
	// if (new_snapshot.has_parent_snapshot) {
	// 	if (!alters.empty()) {
	// 		auto &last_alter = alters.back().get();
	// 		new_snapshot.parent_snapshot_id = last_alter.snapshot.snapshot_id;
	// 	} else {
	// 		D_ASSERT(table_info.table_metadata.has_current_snapshot);
	// 		new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
	// 	}
	// }
	//
	// auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path,
	//                                                   std::move(new_snapshot));
	// auto &delete_manifest_file = add_snapshot->manifest_file;
	// auto &delete_manifest = add_snapshot->manifest;
	// auto &snapshot = add_snapshot->snapshot;
	//
	// delete_manifest.manifest_path = delete_manifest_file_path;
	// delete_manifest.sequence_number = sequence_number;
	// delete_manifest.content = IcebergManifestContentType::DELETE;
	// delete_manifest.added_files_count = delete_files.size();
	// delete_manifest.deleted_files_count = 0;
	// delete_manifest.existing_files_count = 0;
	// delete_manifest.added_rows_count = 0;
	// delete_manifest.existing_rows_count = 0;
	// //! TODO: support partitions
	// delete_manifest.partition_spec_id = 0;
	//
	// // Add delete files to the delete manifest
	// vector<IcebergManifestEntry> iceberg_delete_files;
	// for (auto &delete_entry : delete_files) {
	// 	auto data_file_name = delete_entry.first;
	// 	auto &delete_file = delete_entry.second;
	//
	// 	IcebergManifestEntry manifest_entry;
	// 	manifest_entry.status = IcebergManifestEntryStatusType::ADDED;
	// 	manifest_entry.content = IcebergManifestEntryContentType::POSITION_DELETES;
	// 	manifest_entry.file_path = delete_file.file_name;
	// 	manifest_entry.file_format = "parquet";
	// 	manifest_entry.record_count = delete_file.delete_count;
	// 	manifest_entry.file_size_in_bytes = delete_file.file_size_bytes;
	// 	manifest_entry.sequence_number = snapshot.sequence_number;
	// 	manifest_entry.snapshot_id = snapshot.snapshot_id;
	//
	// 	// set lower and upper bound for the filename column
	// 	manifest_entry.lower_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
	// 	manifest_entry.upper_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
	// 	// set referenced_data_file
	// 	manifest_entry.referenced_data_file = data_file_name;
	// 	iceberg_delete_files.push_back(manifest_entry);
	//
	// 	delete_manifest.deleted_rows_count += delete_file.delete_count;
	// 	manifest_entry.partition_spec_id = delete_manifest.partition_spec_id;
	//
	// 	if (!delete_manifest.has_min_sequence_number || manifest_entry.sequence_number <
	// delete_manifest.min_sequence_number) { 		delete_manifest.min_sequence_number = manifest_entry.sequence_number;
	// 	}
	// 	delete_manifest.has_min_sequence_number = true;
	// }
	// delete_manifest_file.data_files.insert(delete_manifest_file.data_files.end(),
	// std::make_move_iterator(iceberg_delete_files.begin()), 							std::make_move_iterator(iceberg_delete_files.end()));
	//
	// // now create the data manifest
	// auto data_manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	// auto data_manifest_file_path = table_metadata.GetMetadataPath() + "/" + data_manifest_file_uuid + "-m0.avro";
	// IcebergManifestFile data_manifest_file(data_manifest_file_path);
	// // data entry in the manifest list for the snapshot
	// IcebergManifest data_manifest;
	//
	// data_manifest.manifest_path = data_manifest_file_path;
	// data_manifest.sequence_number = sequence_number;
	// data_manifest.content = IcebergManifestContentType::DATA;
	// data_manifest.added_files_count = data_files.size();
	// data_manifest.deleted_files_count = 0;
	// data_manifest.existing_files_count = 0;
	// data_manifest.added_rows_count = 0;
	// data_manifest.existing_rows_count = 0;
	// //! TODO: support partitions
	// data_manifest.partition_spec_id = 0;
	// //! manifest.partitions = CreateManifestPartition();
	//
	// //! Add the data files
	// for (auto &data_file : data_files) {
	// 	data_manifest.added_rows_count += data_file.record_count;
	// 	data_file.sequence_number = snapshot.sequence_number;
	// 	data_file.snapshot_id = snapshot.snapshot_id;
	// 	data_file.partition_spec_id = data_manifest.partition_spec_id;
	// 	if (!data_manifest.has_min_sequence_number || data_file.sequence_number < data_manifest.min_sequence_number) {
	// 		data_manifest.min_sequence_number = data_file.sequence_number;
	// 	}
	// 	data_manifest.has_min_sequence_number = true;
	// }
	// data_manifest.added_snapshot_id = snapshot.snapshot_id;
	// data_manifest_file.data_files.insert(data_manifest_file.data_files.end(),
	// std::make_move_iterator(data_files.begin()),
	//                                 std::make_move_iterator(data_files.end()));
	//
	// add_snapshot->extra_manifest_files.push_back(std::move(data_manifest_file));
	// add_snapshot->extra_manifests.push_back(std::move(data_manifest));
	// alters.push_back(*add_snapshot);
	// updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::TableAddSchema() {
	updates.push_back(make_uniq<AddSchemaUpdate>(table_info));
}

void IcebergTransactionData::TableAssignUUID() {
	updates.push_back(make_uniq<AssignUUIDUpdate>(table_info));
}

void IcebergTransactionData::TableAddAssertCreate() {
	requirements.push_back(make_uniq<AssertCreateRequirement>(table_info));
}

void IcebergTransactionData::TableAddUpradeFormatVersion() {
	updates.push_back(make_uniq<UpgradeFormatVersion>(table_info));
}

void IcebergTransactionData::TableAddSetCurrentSchema() {
	updates.push_back(make_uniq<SetCurrentSchema>(table_info));
}

void IcebergTransactionData::TableAddPartitionSpec() {
	updates.push_back(make_uniq<AddPartitionSpec>(table_info));
}

void IcebergTransactionData::TableAddSortOrder() {
	updates.push_back(make_uniq<AddSortOrder>(table_info));
}

void IcebergTransactionData::TableSetDefaultSortOrder() {
	updates.push_back(make_uniq<SetDefaultSortOrder>(table_info));
}

void IcebergTransactionData::TableSetDefaultSpec() {
	updates.push_back(make_uniq<SetDefaultSpec>(table_info));
}

void IcebergTransactionData::TableSetProperties(case_insensitive_map_t<string> properties) {
	updates.push_back(make_uniq<SetProperties>(table_info, properties));
}

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info));
}

} // namespace duckdb
