#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

struct IcebergDeleteData {
public:
	IcebergDeleteData(const string &manifest_file_path) : manifest_file_path(manifest_file_path) {
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
	virtual void ToSet(set<idx_t> &out) const = 0;

public:
	//! The 'manifest_file.manifest_path' that this delete came from
	string manifest_file_path;
};

} // namespace duckdb
