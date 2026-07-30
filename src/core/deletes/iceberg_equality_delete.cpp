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

void IcebergMultiFileList::ScanEqualityDeleteFile(const BoundIcebergManifestEntry &bound_manifest_entry,
                                                  DataChunk &source,
                                                  const vector<MultiFileColumnDefinition> &global_columns) const {
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

	deletes.emplace_back(data_file.partition_info, manifest_file.partition_spec_id, data_file.file_path);
	auto &equality_values = deletes.back().equality_values;
	D_ASSERT(result.ColumnCount() == data_file.equality_ids.size());

	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto &field_id = data_file.equality_ids[col_idx];
		auto &vec = result.data[col_idx];
		auto &values = equality_values[field_id];
		values.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			values.push_back(vec.GetValue(i));
		}
	}
}

} // namespace duckdb
