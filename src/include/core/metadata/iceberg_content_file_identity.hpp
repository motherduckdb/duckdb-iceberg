#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"

#include <functional>

namespace duckdb {

//! Manifest-entry identity. A Puffin file can contain multiple independently live deletion vectors.
//! An absent offset identifies an ordinary data or delete file, not every blob at that path.
struct IcebergContentFileIdentity {
	IcebergContentFileIdentity(const string &file_path, optional<int64_t> content_offset = nullopt)
	    : file_path(file_path), content_offset(content_offset) {
	}

	bool operator==(const IcebergContentFileIdentity &other) const {
		return file_path == other.file_path && content_offset == other.content_offset;
	}

	string file_path;
	optional<int64_t> content_offset;
};

struct IcebergContentFileIdentityHash {
	size_t operator()(const IcebergContentFileIdentity &identity) const {
		return std::hash<string>()(identity.file_path) ^
		       (identity.content_offset ? std::hash<int64_t>()(*identity.content_offset) : 0);
	}
};

} // namespace duckdb
