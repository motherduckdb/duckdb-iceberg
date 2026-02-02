//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_avro_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct IcebergAvroMultiFileReader : public MultiFileReader {
public:
	IcebergAvroMultiFileReader(shared_ptr<TableFunctionInfo> function_info) : function_info(std::move(function_info)) {
	}

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

private:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
