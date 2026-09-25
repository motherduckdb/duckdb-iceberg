#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

enum class IcebergDeleteType : uint8_t { POSITIONAL_DELETE, DELETION_VECTOR };

struct IcebergDeleteData {
public:
	IcebergDeleteData(IcebergDeleteType type, const string &file_path) : type(type) {
		source_files.push_back(file_path);
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
	virtual void ToSet(set<idx_t> &out) const = 0;

public:
	IcebergDeleteType type;
	//! Source delete-file paths retained for invalidation when replacing a deletion vector.
	vector<string> source_files;
};

} // namespace duckdb
