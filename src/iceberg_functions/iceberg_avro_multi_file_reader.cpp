#include "iceberg_avro_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

unique_ptr<MultiFileReader> IcebergAvroMultiFileReader::CreateInstance(const TableFunction &table) {
	(void)table;
	return make_uniq<IcebergAvroMultiFileReader>();
}

shared_ptr<MultiFileList> IcebergAvroMultiFileReader::CreateFileList(ClientContext &context,
                                                                     const vector<string> &paths,
                                                                     FileGlobOptions options) {

	vector<OpenFileInfo> open_files;
	for (auto &path : paths) {
		open_files.emplace_back(path);
		open_files.back().extended_info = make_uniq<ExtendedOpenFileInfo>();
		open_files.back().extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		open_files.back().extended_info->options["force_full_download"] = Value::BOOLEAN(true);
	}
	auto res = make_uniq<SimpleMultiFileList>(std::move(open_files));
	return std::move(res);
}

MultiFileColumnDefinition CreateColumnFromFieldId(const DuckLakeFieldId &field_id, bool emit_key_value) {
	MultiFileColumnDefinition column(field_id.Name(), field_id.Type());
	auto &column_data = field_id.GetColumnData();
	if (column_data.initial_default.IsNull()) {
		column.default_expression = make_uniq<ConstantExpression>(Value(field_id.Type()));
	} else {
		column.default_expression = make_uniq<ConstantExpression>(column_data.initial_default);
	}
	column.identifier = Value::INTEGER(NumericCast<int32_t>(field_id.GetFieldIndex().index));
	for (auto &child : field_id.Children()) {
		column.children.push_back(CreateColumnFromFieldId(*child, emit_key_value));
	}
	if (field_id.Type().id() == LogicalTypeId::MAP && emit_key_value) {
		// for maps, insert a dummy "key_value" entry
		MultiFileColumnDefinition key_val("key_value", LogicalTypeId::INVALID);
		key_val.children = std::move(column.children);
		column.children.push_back(std::move(key_val));
	}
	return column;
}

// FIXME: emit_key_value is a work-around for an inconsistency in the MultiFileColumnMapper
vector<MultiFileColumnDefinition> DuckLakeMultiFileReader::ColumnsFromFieldData(const DuckLakeFieldData &field_data,
																				bool emit_key_value) {
	vector<MultiFileColumnDefinition> result;
	for (auto &item : field_data.GetFieldIds()) {
		result.push_back(CreateColumnFromFieldId(*item, emit_key_value));
	}
	return result;
}

bool IcebergAvroMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
									 vector<string> &names, MultiFileReaderBindData &bind_data) {
	// throw InternalException("Need to bind an avro read");

	// original bind coe in avroMultiFileInfo::BindReader
	// auto &field_data = read_info.table.GetFieldData();
	// auto &columns = bind_data.schema;
	// columns = ColumnsFromFieldData(field_data);
	//	bind_data.file_row_number_idx = names.size();
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	// names = read_info.column_names;
	// return_types = read_info.column_types;
	return true;
	// AvroFileReaderOptions options;
	// if (bind_data.file_options.union_by_name) {
	// 	throw NotImplementedException("'union_by_name' not implemented for Avro reader yet");
	// }
	// bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	// bind_data.reader_bind = bind_data.multi_file_reader->BindReader(context, return_types, names, *bind_data.file_list,
	// 																bind_data, options, bind_data.file_options);
	// D_ASSERT(names.size() == return_types.size());
}

} // namespace duckdb
