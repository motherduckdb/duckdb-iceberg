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
                                           const vector<string> &source_names, const vector<int32_t> &equality_ids) {
	// The equality-delete file can physically contain more columns than the reader models for it - e.g. Spark
	// embeds the partition columns next to the equality-key columns - and not necessarily in the same order. So
	// resolve each equality field-id to its physical position in 'source' by name rather than by its position in
	// 'local_columns', which only covers the modeled subset.
	unordered_map<string, column_t> name_to_source_index;
	for (column_t i = 0; i < source_names.size(); i++) {
		name_to_source_index[source_names[i]] = i;
	}

	//! Map from equality field-id to the physical column index in 'source'.
	unordered_map<int32_t, column_t> id_to_column;
	for (auto &col : local_columns) {
		D_ASSERT(!col.identifier.IsNull());
		auto entry = name_to_source_index.find(col.name.GetIdentifierName());
		if (entry == name_to_source_index.end()) {
			continue;
		}
		id_to_column[col.identifier.GetValue<int32_t>()] = entry->second;
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
                                                  const vector<MultiFileColumnDefinition> &local_columns,
                                                  const vector<string> &source_names) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DELETE);
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == source_names.size());

	auto count = source.size();
	if (count == 0) {
		return;
	}

	// make result only reference the columns from source (equality delete file) that have equality_ids
	// mentioned in the manifest file
	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, local_columns, source_names, data_file.equality_ids);

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
