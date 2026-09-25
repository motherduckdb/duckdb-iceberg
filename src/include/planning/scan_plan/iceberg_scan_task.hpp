#pragma once

#include "core/deletes/iceberg_delete_file.hpp"

namespace duckdb {

//! SQL representation shared by iceberg_scan_plan and iceberg_scan_tasks.
struct IcebergScanTaskFormat {
	enum Column : idx_t {
		FILE_PATH,
		FILE_FORMAT,
		FILE_SIZE,
		RECORD_COUNT,
		SEQUENCE_NUMBER,
		FIRST_ROW_ID,
		PARTITION_SPEC_ID,
		PARTITION_CONSTANTS,
		DELETE_FILES,
		SNAPSHOT_ID,
		SCHEMA_ID,
		METADATA,
		SCHEMA,
		COLUMN_COUNT
	};

	static LogicalType DeleteFileType();
	static LogicalType SchemaType(const IcebergTableSchema &schema);
	static child_list_t<LogicalType> Columns(const LogicalType &partition_type, const LogicalType &schema_type);
	static IcebergDeleteFile ReadDeleteFile(const Value &descriptor);
	static OpenFileInfo FileInfo(const string &path, const string &format, int64_t size, optional<int64_t> first_row_id,
	                             optional<int64_t> sequence_number);
};

} // namespace duckdb
