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
                                           const vector<MultiFileColumnDefinition> &local_columns,
                                           const vector<int32_t> &equality_ids) {
	//! Map from column_id to 'local_columns' index, to figure out which columns from the 'source' are relevant here
	// 'local_columns' are columns from the equality delete file.
	// id_to_column -> equality_delete_column_field_id_to_output_column_id
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = i;
	}

	// column_ids we want to slice.
	vector<column_t> column_ids;
	for (auto id : equality_ids) {
		D_ASSERT(id_to_column.count(id));
		column_ids.push_back(id_to_column[id]);
	}
	//! Take only the relevant columns from the source (equality_delete_file)
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

void IcebergMultiFileList::ScanEqualityDeleteFile(const BoundIcebergManifestEntry &bound_manifest_entry,
                                                  DataChunk &source,
                                                  vector<MultiFileColumnDefinition> &local_columns) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DELETE);
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == local_columns.size());

	auto count = source.size();
	if (count == 0) {
		return;
	}

	// make result only reference the columns from source (equality delete file) that have equality_ids
	// mentioned in the manifest file
	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, local_columns, data_file.equality_ids);

	const auto sequence_number = manifest_entry.GetSequenceNumber(manifest_file);
	//! Get or create the equality delete data for this sequence number
	auto &equality_delete_data = GetEqualityDeleteData();
	auto it = equality_delete_data.find(sequence_number);
	if (it == equality_delete_data.end()) {
		it = equality_delete_data.emplace(sequence_number, make_uniq<IcebergEqualityDeleteData>(sequence_number)).first;
	}
	auto &deletes = *it->second;

	deletes.delete_files.emplace_back(data_file.partition_info, manifest_file.partition_spec_id, data_file.file_path);
	auto &equality_values = deletes.delete_files.back().equality_values;
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
