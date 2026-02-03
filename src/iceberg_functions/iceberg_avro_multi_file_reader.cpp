#include "iceberg_avro_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_list.hpp"
#include "duckdb/common/exception.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "iceberg_utils.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {
constexpr column_t IcebergAvroMultiFileReader::PARTITION_SPEC_ID_FIELD_ID;
constexpr column_t IcebergAvroMultiFileReader::SEQUENCE_NUMBER_FIELD_ID;
constexpr column_t IcebergAvroMultiFileReader::MANIFEST_FILE_INDEX_FIELD_ID;

unique_ptr<MultiFileReader> IcebergAvroMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergAvroMultiFileReader>(table.function_info);
}

static MultiFileColumnDefinition CreateManifestFilePartitionsColumn() {
	MultiFileColumnDefinition partitions("partitions", IcebergManifestList::FieldSummaryType());
	partitions.identifier = Value::INTEGER(507);
	partitions.default_expression = make_uniq<ConstantExpression>(Value(partitions.type));

	MultiFileColumnDefinition field_summary("field_summary", ListType::GetChildType(partitions.type));
	field_summary.identifier = Value::INTEGER(508);

	//! contains_null - required
	field_summary.children.emplace_back("contains_null", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(509);

	//! contains_nan - optional
	field_summary.children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	field_summary.children.back().identifier = Value::INTEGER(518);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BOOLEAN));

	//! lower_bound - optional
	field_summary.children.emplace_back("lower_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(510);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BLOB));

	//! upper_bound - optional
	field_summary.children.emplace_back("upper_bound", LogicalType::BLOB);
	field_summary.children.back().identifier = Value::INTEGER(511);
	field_summary.children.back().default_expression = make_uniq<ConstantExpression>(Value(LogicalType::BLOB));

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

	// sequence_number (v2+, field-id 515)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
		sequence_number.identifier = Value::INTEGER(515);
		sequence_number.default_expression = make_uniq<ConstantExpression>(Value(sequence_number.type));
		schema.push_back(sequence_number);
	}

	// min_sequence_number (v2+, field-id 516, default 0)
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition min_sequence_number("min_sequence_number", LogicalType::BIGINT);
		min_sequence_number.identifier = Value::INTEGER(516);
		min_sequence_number.default_expression = make_uniq<ConstantExpression>(Value::BIGINT(0));
		schema.push_back(min_sequence_number);
	}

	// added_snapshot_id (field-id 503)
	MultiFileColumnDefinition added_snapshot_id("added_snapshot_id", LogicalType::BIGINT);
	added_snapshot_id.identifier = Value::INTEGER(503);
	schema.push_back(added_snapshot_id);

	// added_files_count (v2+, field-id 504)
	MultiFileColumnDefinition added_files_count("added_files_count", LogicalType::INTEGER);
	added_files_count.identifier = Value::INTEGER(504);
	schema.push_back(added_files_count);

	// existing_files_count (v2+, field-id 505)
	MultiFileColumnDefinition existing_files_count("existing_files_count", LogicalType::INTEGER);
	existing_files_count.identifier = Value::INTEGER(505);
	schema.push_back(existing_files_count);

	// deleted_files_count (v2+, field-id 506)
	MultiFileColumnDefinition deleted_files_count("deleted_files_count", LogicalType::INTEGER);
	deleted_files_count.identifier = Value::INTEGER(506);
	schema.push_back(deleted_files_count);

	// added_rows_count (v2+, field-id 512)
	MultiFileColumnDefinition added_rows_count("added_rows_count", LogicalType::BIGINT);
	added_rows_count.identifier = Value::INTEGER(512);
	schema.push_back(added_rows_count);

	// existing_rows_count (v2+, field-id 513)
	MultiFileColumnDefinition existing_rows_count("existing_rows_count", LogicalType::BIGINT);
	existing_rows_count.identifier = Value::INTEGER(513);
	schema.push_back(existing_rows_count);

	// deleted_rows_count (v2+, field-id 514)
	MultiFileColumnDefinition deleted_rows_count("deleted_rows_count", LogicalType::BIGINT);
	deleted_rows_count.identifier = Value::INTEGER(514);
	schema.push_back(deleted_rows_count);

	// partitions (v2+, field-id 507)
	schema.push_back(CreateManifestFilePartitionsColumn());

	// first_row_id (v3+, field-id 520, default 0)
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(520);
		first_row_id.default_expression = make_uniq<ConstantExpression>(Value(first_row_id.type));
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
		partition_field.default_expression = make_uniq<ConstantExpression>(Value(type));
		partition.children.push_back(partition_field);
	}
	return partition;
}

static vector<MultiFileColumnDefinition> BuildManifestSchema(const IcebergSnapshot &snapshot,
                                                             const IcebergTableMetadata &metadata,
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
	snapshot_id.default_expression = make_uniq<ConstantExpression>(Value(snapshot_id.type));
	schema.push_back(snapshot_id);

	// sequence_number (optional, field-id 3)
	MultiFileColumnDefinition sequence_number("sequence_number", LogicalType::BIGINT);
	sequence_number.identifier = Value::INTEGER(3);
	sequence_number.default_expression = make_uniq<ConstantExpression>(Value(sequence_number.type));
	schema.push_back(sequence_number);

	// file_sequence_number (optional, field-id 4)
	MultiFileColumnDefinition file_sequence_number("file_sequence_number", LogicalType::BIGINT);
	file_sequence_number.identifier = Value::INTEGER(4);
	file_sequence_number.default_expression = make_uniq<ConstantExpression>(Value(file_sequence_number.type));
	schema.push_back(file_sequence_number);

	//! Map all the referenced partition spec ids to the partition fields that *could* be referenced,
	//! any missing fields will be NULL
	auto partition_field_id_to_type = IcebergDataFile::GetFieldIdToTypeMapping(snapshot, metadata, partition_spec_ids);
	auto partition_type = IcebergDataFile::PartitionStructType(partition_field_id_to_type);

	{
		// data_file struct (field-id 2)
		MultiFileColumnDefinition data_file("data_file", IcebergDataFile::GetType(metadata, partition_type));
		data_file.identifier = Value::INTEGER(2);
		schema.push_back(data_file);
	}
	auto data_file_idx = schema.size() - 1;

	//! FIXME: how should these be added so they can be found by lookup
	//! Virtual columns
	// MultiFileColumnDefinition partition_spec_id("partition_spec_id", LogicalType::INTEGER);
	// partition_spec_id.identifier = Value::INTEGER(IcebergAvroMultiFileReader::PARTITION_SPEC_ID_FIELD_ID);
	// partition_spec_id.default_expression = make_uniq<ConstantExpression>(Value(partition_spec_id.type));
	// schema.push_back(partition_spec_id);

	// MultiFileColumnDefinition manifest_file_sequence_number("sequence_number", LogicalType::BIGINT);
	// manifest_file_sequence_number.identifier = Value::INTEGER(IcebergAvroMultiFileReader::SEQUENCE_NUMBER_FIELD_ID);
	// manifest_file_sequence_number.default_expression =
	// make_uniq<ConstantExpression>(Value(manifest_file_sequence_number.type));
	// schema.push_back(manifest_file_sequence_number);

	// MultiFileColumnDefinition manifest_file_index("manifest_file_index", LogicalType::UBIGINT);
	// manifest_file_index.identifier = Value::INTEGER(IcebergAvroMultiFileReader::MANIFEST_FILE_INDEX_FIELD_ID);
	// manifest_file_index.default_expression = make_uniq<ConstantExpression>(Value(manifest_file_index.type));
	// schema.push_back(manifest_file_index);

	auto &data_file = schema[data_file_idx];
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

	// split_offsets - optional
	auto split_offsets_type = LogicalType::LIST(LogicalType::BIGINT);
	MultiFileColumnDefinition split_offsets("split_offsets", split_offsets_type);
	split_offsets.identifier = Value::INTEGER(132);
	split_offsets.default_expression = make_uniq<ConstantExpression>(Value(split_offsets.type));
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(split_offsets_type));
		list.identifier = Value::INTEGER(133);
		split_offsets.children.push_back(list);
	}
	data_file.children.push_back(split_offsets);

	// equality_ids - optional
	auto equality_ids_type = LogicalType::LIST(LogicalType::INTEGER);
	MultiFileColumnDefinition equality_ids("equality_ids", equality_ids_type);
	equality_ids.identifier = Value::INTEGER(135);
	equality_ids.default_expression = make_uniq<ConstantExpression>(Value(equality_ids.type));
	{
		MultiFileColumnDefinition list("list", ListType::GetChildType(equality_ids_type));
		list.identifier = Value::INTEGER(136);
		equality_ids.children.push_back(list);
	}
	data_file.children.push_back(equality_ids);

	// sort_order_id - optional
	MultiFileColumnDefinition sort_order_id("sort_order_id", LogicalType::INTEGER);
	sort_order_id.identifier = Value::INTEGER(140);
	sort_order_id.default_expression = make_uniq<ConstantExpression>(Value(sort_order_id.type));
	data_file.children.push_back(sort_order_id);

	// first_row_id
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition first_row_id("first_row_id", LogicalType::BIGINT);
		first_row_id.identifier = Value::INTEGER(142);
		first_row_id.default_expression = make_uniq<ConstantExpression>(Value(first_row_id.type));
		data_file.children.push_back(first_row_id);
	}

	// referenced_data_file - optional
	if (iceberg_version >= 2) {
		MultiFileColumnDefinition referenced_data_file("referenced_data_file", LogicalType::VARCHAR);
		referenced_data_file.identifier = Value::INTEGER(143);
		referenced_data_file.default_expression = make_uniq<ConstantExpression>(Value(referenced_data_file.type));
		data_file.children.push_back(referenced_data_file);
	}

	// content_offset - optional
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_offset("content_offset", LogicalType::BIGINT);
		content_offset.identifier = Value::INTEGER(144);
		content_offset.default_expression = make_uniq<ConstantExpression>(Value(content_offset.type));
		data_file.children.push_back(content_offset);
	}

	// content_size_in_bytes - optional
	if (iceberg_version >= 3) {
		MultiFileColumnDefinition content_size_in_bytes("content_size_in_bytes", LogicalType::BIGINT);
		content_size_in_bytes.identifier = Value::INTEGER(145);
		content_size_in_bytes.default_expression = make_uniq<ConstantExpression>(Value(content_size_in_bytes.type));
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
	auto &type = scan_info.type;
	auto &metadata = scan_info.metadata;
	auto &snapshot = scan_info.snapshot;

	// Build the expected schema with field IDs
	vector<MultiFileColumnDefinition> schema;
	if (type == AvroScanInfoType::MANIFEST_LIST) {
		schema = BuildManifestListSchema(metadata);
	} else {
		auto &manifest_file_scan = scan_info.Cast<IcebergManifestFileScanInfo>();
		auto &manifest_files = manifest_file_scan.manifest_files;
		unordered_set<int32_t> partition_spec_ids;
		for (auto &manifest_file : manifest_files) {
			partition_spec_ids.insert(manifest_file.partition_spec_id);
		}
		schema = BuildManifestSchema(snapshot, metadata, partition_spec_ids);
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

unique_ptr<Expression> IcebergAvroMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    idx_t &column_id, const LogicalType &type, MultiFileLocalIndex local_idx,
    optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	if (column_id == PARTITION_SPEC_ID_FIELD_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for partition_spec_id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("partition_spec_id");
		if (entry == options.end()) {
			throw InternalException("'partition_spec_id' not set when initializing the FileList");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	if (column_id == SEQUENCE_NUMBER_FIELD_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for sequence number column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("sequence_number");
		if (entry == options.end()) {
			throw InternalException("'sequence_number' not set when initializing the FileList");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	if (column_id == MANIFEST_FILE_INDEX_FIELD_ID) {
		return make_uniq<BoundConstantExpression>(Value::UBIGINT(local_idx.GetIndex()));
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx,
	                                                   global_column_reference);
}

shared_ptr<MultiFileList> IcebergAvroMultiFileReader::CreateFileList(ClientContext &context,
                                                                     const vector<string> &paths,
                                                                     const FileGlobInput &glob_input) {
	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergAvroScanInfo>(function_info);
	vector<OpenFileInfo> open_files;

	if (scan_info->type == AvroScanInfoType::MANIFEST_LIST) {
		D_ASSERT(paths.size() == 1);
		open_files.emplace_back(paths[0]);
		auto &file_info = open_files.back();
		file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
		//! TODO: lookup or assign the associated 'IcebergManifestFile' entry for each manifest (if not scanning a
		//! manifest-list)
		// file_info.extended_info->options["file_size"] = Value::UBIGINT(manifest.manifest_length);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	} else {
		auto &manifest_files_scan = scan_info->Cast<IcebergManifestFileScanInfo>();
		auto &manifest_files = manifest_files_scan.manifest_files;
		auto &options = manifest_files_scan.options;
		auto &fs = manifest_files_scan.fs;
		auto &iceberg_path = manifest_files_scan.iceberg_path;
		for (auto &manifest : manifest_files) {
			auto full_path = options.allow_moved_paths
			                     ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
			                     : manifest.manifest_path;
			open_files.emplace_back(full_path);
			auto &file_info = open_files.back();
			file_info.extended_info = make_uniq<ExtendedOpenFileInfo>();
			file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
			file_info.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
			file_info.extended_info->options["file_size"] = Value::UBIGINT(manifest.manifest_length);
			file_info.extended_info->options["etag"] = Value("");
			file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
			file_info.extended_info->options["partition_spec_id"] = Value::INTEGER(manifest.partition_spec_id);
			file_info.extended_info->options["sequence_number"] = Value::BIGINT(manifest.sequence_number);
		}
	}

	auto res = make_uniq<IcebergAvroMultiFileList>(scan_info, std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
