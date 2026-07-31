#include "core/deletes/iceberg_equality_delete.hpp"

#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto &id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

static void ColumnsReferencedByEqualityIds(DataChunk &source, DataChunk &result,
                                           const vector<MultiFileColumnDefinition> &global_columns,
                                           const vector<int32_t> &equality_ids) {
	D_ASSERT(source.ColumnCount() == global_columns.size());

	//! The equality scan maps every physical file to this global schema by Iceberg field-id. Map the equality ids
	//! for this particular delete file back to their positions in the global output chunk.
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t column_idx = 0; column_idx < global_columns.size(); column_idx++) {
		auto &col = global_columns[column_idx];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = column_idx;
	}

	// column_ids we want to slice.
	vector<column_t> column_ids;
	for (auto id : equality_ids) {
		auto entry = id_to_column.find(id);
		if (entry == id_to_column.end()) {
			throw InternalException("Equality-delete field id %d is missing from the global delete schema", id);
		}
		column_ids.push_back(entry->second);
	}
	//! Take only the relevant columns from the source (equality_delete_file)
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

static IcebergEqualityDeleteFile &GetOrCreateEqualityDeleteFile(vector<unique_ptr<IcebergEqualityDeleteFile>> &deletes,
                                                                idx_t manifest_entry_index,
                                                                const BoundIcebergManifestEntry &manifest_entry,
                                                                sequence_number_t sequence_number,
                                                                equality_delete_file_index_map_t &file_indexes) {
	auto &data_file = manifest_entry.entry.data_file;
	auto &sequence_indexes = file_indexes[sequence_number];
	auto index_entry = sequence_indexes.find(data_file.file_path);
	if (index_entry != sequence_indexes.end()) {
		if (index_entry->second >= deletes.size()) {
			throw InternalException("Equality-delete file index %llu is out of bounds for sequence number %lld",
			                        index_entry->second, sequence_number);
		}
		auto &delete_file = *deletes[index_entry->second];
		return delete_file;
	}

	auto delete_index = deletes.size();
	deletes.push_back(make_uniq<IcebergEqualityDeleteFile>(manifest_entry_index));
	sequence_indexes.emplace(data_file.file_path, delete_index);
	return *deletes.back();
}

void IcebergMultiFileList::ScanEqualityDeleteFile(idx_t manifest_entry_index,
                                                  const BoundIcebergManifestEntry &bound_manifest_entry,
                                                  DataChunk &source,
                                                  const vector<MultiFileColumnDefinition> &global_columns,
                                                  equality_delete_file_index_map_t &file_indexes) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DELETE);
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == global_columns.size());

	auto count = source.size();
	if (count == 0) {
		return;
	}

	// make result only reference the columns from source (equality delete file) that have equality_ids
	// mentioned in the manifest file
	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, global_columns, data_file.equality_ids);

	const auto sequence_number = manifest_entry.GetSequenceNumber(manifest_file);
	//! Get or create the equality delete data for this sequence number
	auto &equality_delete_data = GetEqualityDeleteData();
	auto &deletes = equality_delete_data[sequence_number];

	auto &delete_file = GetOrCreateEqualityDeleteFile(deletes, manifest_entry_index, bound_manifest_entry,
	                                                  sequence_number, file_indexes);
	auto &equality_values = delete_file.equality_values;
	D_ASSERT(result.ColumnCount() == data_file.equality_ids.size());
	if (data_file.record_count < 0) {
		throw InvalidConfigurationException("Equality delete file '%s' has a negative record count",
		                                    data_file.file_path);
	}
	auto expected_row_count = NumericCast<idx_t>(data_file.record_count);
	if (equality_values.size() > expected_row_count || count > expected_row_count - equality_values.size()) {
		throw InvalidConfigurationException(
		    "Equality delete file '%s' contains more rows than its record count of %llu", data_file.file_path,
		    expected_row_count);
	}
	if (equality_values.ColumnCount() == 0) {
		equality_values.Initialize(context, result.GetTypes(), expected_row_count);
	} else {
		if (equality_values.ColumnCount() != result.ColumnCount()) {
			throw InvalidConfigurationException("Equality delete file '%s' produced chunks with differing schemas",
			                                    data_file.file_path);
		}
		for (idx_t column_idx = 0; column_idx < result.ColumnCount(); column_idx++) {
			if (equality_values.data[column_idx].GetType() != result.data[column_idx].GetType()) {
				throw InvalidConfigurationException("Equality delete file '%s' produced chunks with differing schemas",
				                                    data_file.file_path);
			}
		}
	}
	equality_values.Append(result, VectorAppendMode::ERROR_ON_NO_SPACE);
}

} // namespace duckdb
