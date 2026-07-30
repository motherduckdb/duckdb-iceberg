//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

// Pass the already resolved open-file information to the delete scan.
struct IcebergDeleteScanInfo : public TableFunctionInfo {
public:
	IcebergDeleteScanInfo(vector<OpenFileInfo> file_infos) : file_infos(std::move(file_infos)) {
	}

public:
	vector<OpenFileInfo> file_infos;
};

struct IcebergDeleteFileReader : public MultiFileReader {
	IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

public:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
