#include "iceberg_avro_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_list.hpp"
#include "duckdb/common/exception.hpp"
#include "metadata/iceberg_manifest_list.hpp"

namespace duckdb {

unique_ptr<MultiFileReader> IcebergAvroMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergAvroMultiFileReader>(table.function_info);
}

static MultiFileColumnDefinition CreateManifestFilePartitionsColumn() {
	MultiFileColumnDefinition partitions("partitions", IcebergManifestList::FieldSummaryType());
	partitions.identifier = Value::INTEGER(507);

	MultiFileColumnDefinition field_summary("field_summary", ListType::GetChildType(partitions.type));
	field_summary.identifier = Value::INTEGER(508);

	field_summary.children.emplace_back("contains_null", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(509);
	field_summary.children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(518);
	field_summary.children.emplace_back("lower_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(510);
	field_summary.children.emplace_back("upper_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(511);

	partitions.children.push_back(field_summary);
	return partitions;
}

static vector<MultiFileColumnDefinition> BuildManifestListSchema(const IcebergTableMetadata &metadata) {
	vector<MultiFileColumnDefinition> schema;

	auto &iceberg_version = metadata.iceberg_version;
	// manifest_path (required, field-id 500)
	MultiFileColumnDefinition manifest_path("manifest_path", LogicalType::VARCHAR);
	manifest_path.identifier = Value::INTEGER(500);
	schema.push_back(manifest_path);

	// manifest_length (required, field-id 501)
	MultiFileColumnDefinition manifest_length("manifest_length", LogicalType::BIGINT);
	manifest_length.identifier = Value::INTEGER(501);
	schema.push_back(manifest_length);

	// partition_spec_id (optional, field-id 502, default 0)
	MultiFileColumnDefinition partition_spec_id("partition_spec_id", LogicalType::INTEGER);
	partition_spec_id.identifier = Value::INTEGER(502);
	schema.push_back(partition_spec_id);

	// content (v2+, field-id 517, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition content("content", LogicalType::INTEGER);
		content.identifier = Value::INTEGER(517);
		content.default_expression = make_uniq<ConstantExpression>(Value::INTEGER(0));
		schema.push_back(content);
	}

	// sequence_number (v2+, field-id 515, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
		sequence_number.identifier = Value::INTEGER(515);
		sequence_number.default_expression = make_uniq<ConstantExpression>(Value::BIGINT(0));
		schema.push_back(sequence_number);
	}

	// min_sequence_number (v2+, field-id 516, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition min_sequence_number("min_sequence_number", LogicalType::BIGINT);
		min_sequence_number.identifier = Value::INTEGER(516);
		min_sequence_number.default_expression = make_uniq<ConstantExpression>(Value::BIGINT(0));
		schema.push_back(min_sequence_number);
	}

	// added_snapshot_id (field-id 503, default 0)
	MultiFileColumnDefinition added_snapshot_id("added_snapshot_id", LogicalType::BIGINT);
	added_snapshot_id.identifier = Value::INTEGER(503);
	schema.push_back(added_snapshot_id);

	// added_files_count (v2+, field-id 504, default 0)
	MultiFileColumnDefinition added_files_count("added_files_count", LogicalType::INTEGER);
	added_files_count.identifier = Value::INTEGER(504);
	schema.push_back(added_files_count);

	// existing_files_count (v2+, field-id 505, default 0)
	MultiFileColumnDefinition existing_files_count("existing_files_count", LogicalType::INTEGER);
	existing_files_count.identifier = Value::INTEGER(505);
	schema.push_back(existing_files_count);

	// deleted_files_count (v2+, field-id 506, default 0)
	MultiFileColumnDefinition deleted_files_count("deleted_files_count", LogicalType::INTEGER);
	deleted_files_count.identifier = Value::INTEGER(506);
	schema.push_back(deleted_files_count);

	// added_rows_count (v2+, field-id 512, default 0)
	MultiFileColumnDefinition added_rows_count("added_rows_count", LogicalType::BIGINT);
	added_rows_count.identifier = Value::INTEGER(512);
	schema.push_back(added_rows_count);

	// existing_rows_count (v2+, field-id 513, default 0)
	MultiFileColumnDefinition existing_rows_count("existing_rows_count", LogicalType::BIGINT);
	existing_rows_count.identifier = Value::INTEGER(513);
	schema.push_back(existing_rows_count);

	// deleted_rows_count (v2+, field-id 514, default 0)
	MultiFileColumnDefinition deleted_rows_count("deleted_rows_count", LogicalType::BIGINT);
	deleted_rows_count.identifier = Value::INTEGER(514);
	schema.push_back(deleted_rows_count);

	// partitions (v2+, field-id 507, default 0)
	schema.push_back(CreateManifestFilePartitionsColumn());

	// first_row_id (v3+, field-id 520, default 0)
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(520);
		schema.push_back(first_row_id);
	}
	return schema;
}

static MultiFileColumnDefinition CreateManifestPartitionColumn(const map<idx_t, LogicalType> partition_field_id_to_type,
                                                               const LogicalType &partition_type) {
	MultiFileColumnDefinition partition("partition", partition_type);
	partition.identifier = Value::INTEGER(102);
	for (auto &it : partition_field_id_to_type) {
		auto partition_field_id = it.first;
		auto &type = it.second;

		MultiFileColumnDefinition partition_field(StringUtil::Format("r%d", partition_field_id), type);
		partition_field.identifier = Value::INTEGER(partition_field_id);
		partition.children.push_back(partition_field);
	}
	return partition;
}

static vector<MultiFileColumnDefinition> BuildManifestSchema(const IcebergTableMetadata &metadata,
                                                             const unordered_set<int32_t> &partition_spec_ids) {
	vector<MultiFileColumnDefinition> schema;

	auto &iceberg_version = metadata.iceberg_version;
	// status (required, field-id 0)
	MultiFileColumnDefinition status("status", LogicalType::INTEGER);
	status.identifier = Value::INTEGER(0);
	schema.push_back(status);

	// snapshot_id (optional, field-id 1)
	MultiFileColumnDefinition snapshot_id("snapshot_id", LogicalType::BIGINT);
	snapshot_id.identifier = Value::INTEGER(1);
	schema.push_back(snapshot_id);

	// sequence_number (optional, field-id 3)
	MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
	sequence_number.identifier = Value::INTEGER(3);
	schema.push_back(sequence_number);

	// file_sequence_number (optional, field-id 4)
	MultiFileColumnDefinition file_sequence_number("file_sequence_number", LogicalType::BIGINT);
	file_sequence_number.identifier = Value::INTEGER(4);
	schema.push_back(file_sequence_number);

	//! Map all the referenced partition spec ids to the partition fields that *could* be referenced,
	//! any missing fields will be NULL
	auto partition_field_id_to_type = IcebergDataFile::GetFieldIdToTypeMapping(metadata, partition_spec_ids);
	auto partition_type = IcebergDataFile::PartitionStructType(partition_field_id_to_type);

	// data_file struct (field-id 2)
	MultiFileColumnDefinition data_file("data_file", IcebergDataFile::GetType(metadata, partition_type));
	data_file.identifier = Value::INTEGER(2);

	// Add children with their field IDs
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition content("content", LogicalType::INTEGER);
		content.identifier = Value::INTEGER(134);
		data_file.children.push_back(content);
	}

	MultiFileColumnDefinition file_path("file_path", LogicalType::VARCHAR);
	file_path.identifier = Value::INTEGER(100);
	data_file.children.push_back(file_path);

	MultiFileColumnDefinition file_format("file_format", LogicalType::VARCHAR);
	file_format.identifier = Value::INTEGER(101);
	data_file.children.push_back(file_format);

	data_file.children.push_back(CreateManifestPartitionColumn(partition_field_id_to_type, partition_type));

	MultiFileColumnDefinition record_count("record_count", LogicalType::BIGINT);
	record_count.identifier = Value::INTEGER(103);
	data_file.children.push_back(record_count);

	MultiFileColumnDefinition file_size_in_bytes("file_size_in_bytes", LogicalType::BIGINT);
	file_size_in_bytes.identifier = Value::INTEGER(104);
	data_file.children.push_back(file_size_in_bytes);

	// column_sizes
	auto column_sizes_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition column_sizes("column_sizes", column_sizes_type);
	column_sizes.identifier = Value::INTEGER(108);
	column_sizes.default_expression = make_uniq<ConstantExpression>(Value(column_sizes_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(column_sizes_type));
		key.identifier = Value::INTEGER(117);
		column_sizes.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(column_sizes_type));
		value.identifier = Value::INTEGER(118);
		column_sizes.children.push_back(value);
	}
	data_file.children.push_back(column_sizes);

	// value_counts
	auto value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition value_counts("value_counts", value_counts_type);
	value_counts.identifier = Value::INTEGER(109);
	value_counts.default_expression = make_uniq<ConstantExpression>(Value(value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(value_counts_type));
		key.identifier = Value::INTEGER(119);
		value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(value_counts_type));
		value.identifier = Value::INTEGER(120);
		value_counts.children.push_back(value);
	}
	data_file.children.push_back(value_counts);

	// null_value_counts
	auto null_value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition null_value_counts("null_value_counts", null_value_counts_type);
	null_value_counts.identifier = Value::INTEGER(110);
	null_value_counts.default_expression = make_uniq<ConstantExpression>(Value(null_value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(null_value_counts_type));
		key.identifier = Value::INTEGER(121);
		null_value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(null_value_counts_type));
		value.identifier = Value::INTEGER(122);
		null_value_counts.children.push_back(value);
	}
	data_file.children.push_back(null_value_counts);

	// nan_value_counts
	auto nan_value_counts_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	MultiFileColumnDefinition nan_value_counts("nan_value_counts", nan_value_counts_type);
	nan_value_counts.identifier = Value::INTEGER(138);
	nan_value_counts.default_expression = make_uniq<ConstantExpression>(Value(nan_value_counts_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(nan_value_counts_type));
		key.identifier = Value::INTEGER(138);
		nan_value_counts.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(nan_value_counts_type));
		value.identifier = Value::INTEGER(139);
		nan_value_counts.children.push_back(value);
	}
	data_file.children.push_back(nan_value_counts);

	// lower_bounds
	auto lower_bounds_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB);
	MultiFileColumnDefinition lower_bounds("lower_bounds", lower_bounds_type);
	lower_bounds.identifier = Value::INTEGER(125);
	lower_bounds.default_expression = make_uniq<ConstantExpression>(Value(lower_bounds_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(lower_bounds_type));
		key.identifier = Value::INTEGER(126);
		lower_bounds.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(lower_bounds_type));
		value.identifier = Value::INTEGER(127);
		lower_bounds.children.push_back(value);
	}
	data_file.children.push_back(lower_bounds);

	// upper_bounds
	auto upper_bounds_type = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB);
	MultiFileColumnDefinition upper_bounds("upper_bounds", upper_bounds_type);
	upper_bounds.identifier = Value::INTEGER(128);
	upper_bounds.default_expression = make_uniq<ConstantExpression>(Value(upper_bounds_type));
	{
		MultiFileColumnDefinition key("key", MapType::KeyType(upper_bounds_type));
		key.identifier = Value::INTEGER(129);
		upper_bounds.children.push_back(key);

		MultiFileColumnDefinition value("value", MapType::ValueType(upper_bounds_type));
		value.identifier = Value::INTEGER(130);
		upper_bounds.children.push_back(value);
	}
	data_file.children.push_back(upper_bounds);

	// split_offsets
	auto split_offsets_type = LogicalType::LIST(LogicalType::BIGINT);
	MultiFileColumnDefinition split_offsets("split_offsets", split_offsets_type);
	split_offsets.identifier = Value::INTEGER(132);
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(split_offsets_type));
		list.identifier = Value::INTEGER(133);
		split_offsets.children.push_back(list);
	}
	data_file.children.push_back(split_offsets);

	// equality_ids
	auto equality_ids_type = LogicalType::LIST(LogicalType::INTEGER);
	MultiFileColumnDefinition equality_ids("equality_ids", equality_ids_type);
	equality_ids.identifier = Value::INTEGER(135);
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(equality_ids_type));
		list.identifier = Value::INTEGER(136);
		equality_ids.children.push_back(list);
	}
	data_file.children.push_back(equality_ids);

	// sort_id
	MultiFileColumnDefinition sort_order_id("sort_order_id", LogicalType::INTEGER);
	sort_order_id.identifier = Value::INTEGER(140);
	data_file.children.push_back(sort_order_id);

	// first_row_id
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(142);
		data_file.children.push_back(first_row_id);
	}

	// referenced_data_file
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition referenced_data_file("referenced_data_file", LogicalType::VARCHAR);
		referenced_data_file.identifier = Value::INTEGER(143);
		data_file.children.push_back(referenced_data_file);
	}

	// content_offset
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_offset("content_offset", LogicalType::BIGINT);
		content_offset.identifier = Value::INTEGER(144);
		data_file.children.push_back(content_offset);
	}

	// content_size_in_bytes
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_size_in_bytes("content_size_in_bytes", LogicalType::BIGINT);
		content_size_in_bytes.identifier = Value::INTEGER(145);
		data_file.children.push_back(content_size_in_bytes);
	}

	schema.push_back(data_file);
	return schema;
}

bool IcebergAvroMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files,
                                      vector<LogicalType> &return_types, vector<string> &names,
                                      MultiFileReaderBindData &bind_data) {
	auto &iceberg_avro_list = dynamic_cast<IcebergAvroMultiFileList &>(files);

	// Determine if we're reading manifest-list or manifest based on context
	auto &scan_info = iceberg_avro_list.info->Cast<IcebergAvroScanInfo>();
	auto &is_manifest_list = scan_info.is_manifest_list;
	auto &metadata = scan_info.metadata;
	auto &partition_spec_ids = scan_info.partition_spec_ids;

	// Build the expected schema with field IDs
	vector<MultiFileColumnDefinition> schema;
	if (is_manifest_list) {
		schema = BuildManifestListSchema(metadata);
	} else {
		schema = BuildManifestSchema(metadata, partition_spec_ids);
	}

	// Populate return_types and names from schema
	for (auto &col : schema) {
		return_types.push_back(col.type);
		names.push_back(col.name);
	}

	// Set the schema in bind_data - framework will use this for mapping
	bind_data.schema = std::move(schema);
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;

	return true;
}

shared_ptr<MultiFileList> IcebergAvroMultiFileReader::CreateFileList(ClientContext &context,
                                                                     const vector<string> &paths,
                                                                     const FileGlobInput &glob_input) {
	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergAvroScanInfo>(function_info);
	vector<OpenFileInfo> open_files;
	for (auto &path : paths) {
		open_files.emplace_back(path);
		auto &file_info = open_files.back();
		file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
		//! TODO: lookup or assign the associated 'IcebergManifestFile' entry for each manifest (if not scanning a
		//! manifest-list)
		// file_info.extended_info->options["file_size"] = Value::UBIGINT(manifest.manifest_length);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	}

	auto res = make_uniq<IcebergAvroMultiFileList>(scan_info, std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
