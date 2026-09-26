#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "core/metadata/iceberg_content_file_identity.hpp"

namespace duckdb {

enum class IcebergDeleteType : uint8_t { POSITIONAL_DELETE, DELETION_VECTOR };

struct IcebergDeleteData {
public:
	IcebergDeleteData(IcebergDeleteType type, const IcebergContentFileIdentity &file) : type(type) {
		source_files.push_back(file);
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
	virtual void ToSet(set<idx_t> &out) const = 0;

public:
	IcebergDeleteType type;
	//! Original content identities retained for invalidation when replacing a deletion vector.
	vector<IcebergContentFileIdentity> source_files;
};

} // namespace duckdb
