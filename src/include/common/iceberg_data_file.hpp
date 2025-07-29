
// TODO: move this to storage/iceberg_metadata_info.hpp
#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct IcebergDeleteFile {
	// TODO: should this be a data file index?
	DataFileIndex data_file_id;
	string data_file_path;
	string file_name;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	string encryption_key;
	bool overwrites_existing_delete = false;
};

} // namespace duckdb
