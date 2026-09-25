#pragma once

#include "core/deletes/iceberg_delete_file.hpp"

namespace duckdb {

//! Materialized input to a single-file scan, independent of its SQL representation.
struct IcebergFileScanTask {
	string file_path;
	string file_format;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	optional<int64_t> sequence_number;
	optional<int64_t> first_row_id;
	int32_t partition_spec_id = 0;
	unordered_map<int32_t, Value> partition_constants;
	vector<IcebergDeleteFile> delete_files;
};

} // namespace duckdb
