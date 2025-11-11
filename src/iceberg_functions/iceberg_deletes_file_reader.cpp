#include "iceberg_functions/iceberg_deletes_file_reader.hpp"

namespace duckdb {

IcebergDeleteFileReader::IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info)
    : function_info(function_info) {
}

unique_ptr<MultiFileReader> IcebergDeleteFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergDeleteFileReader>(table.function_info);
}

shared_ptr<MultiFileList> IcebergDeleteFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                  const FileGlobInput &glob_input) {

	D_ASSERT(paths.size() == 1);
	vector<OpenFileInfo> open_files;
	D_ASSERT(function_info);
	auto &iceberg_delete_function_info = function_info->Cast<ParquetDeleteScanInfo>();
	auto &extended_delete_info = iceberg_delete_function_info.file_info;
	open_files.emplace_back(extended_delete_info);
	auto res = make_uniq<SimpleMultiFileList>(std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
