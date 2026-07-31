#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct IcebergEqualityDeleteFile {
public:
	explicit IcebergEqualityDeleteFile(vector<int32_t> equality_ids_p) : equality_ids(std::move(equality_ids_p)) {
	}
	IcebergEqualityDeleteFile(const IcebergEqualityDeleteFile &) = delete;
	IcebergEqualityDeleteFile &operator=(const IcebergEqualityDeleteFile &) = delete;

public:
	//! Columns in equality_values follow this field-id order.
	vector<int32_t> equality_ids;
	DataChunk equality_values;
};

} // namespace duckdb
