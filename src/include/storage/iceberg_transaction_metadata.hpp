#pragma once

#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct IcebergManifestDeletes {
public:
	IcebergManifestDeletes() {
	}

public:
	//! The 'data_file.file_path' of deleted files
	case_insensitive_set_t deleted_data_files;
};

} // namespace duckdb
