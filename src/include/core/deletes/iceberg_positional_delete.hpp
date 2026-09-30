#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "core/deletes/iceberg_delete_data.hpp"
#include <roaring/roaring64.h>

namespace duckdb {

struct IcebergPositionalDeleteData : public enable_shared_from_this<IcebergPositionalDeleteData>, IcebergDeleteData {
public:
	IcebergPositionalDeleteData(const IcebergFileIdentity &file)
	    : IcebergDeleteData(IcebergDeleteType::POSITIONAL_DELETE, file),
	      invalid_rows(roaring::api::roaring64_bitmap_create(), roaring::api::roaring64_bitmap_free) {
		if (!invalid_rows) {
			throw OutOfMemoryException("Failed to allocate Iceberg positional delete bitmap");
		}
	}
	virtual ~IcebergPositionalDeleteData() override {
	}

public:
	void AddRow(int64_t row_id) {
		roaring::api::roaring64_bitmap_add(invalid_rows.get(), static_cast<uint64_t>(row_id));
	}
	void MergeRows(const IcebergPositionalDeleteData &other) {
		roaring::api::roaring64_bitmap_or_inplace(invalid_rows.get(), other.invalid_rows.get());
	}
	unique_ptr<DeleteFilter> ToFilter() const override;
	void ToSet(set<idx_t> &out) const override;

public:
	//! Compressed positions retaining all 64 bits of each row ID.
	unique_ptr<roaring::api::roaring64_bitmap_t, decltype(&roaring::api::roaring64_bitmap_free)> invalid_rows;
};

struct IcebergPositionalDeleteFilter : public DeleteFilter {
public:
	IcebergPositionalDeleteFilter(shared_ptr<const IcebergPositionalDeleteData> data) : data(data) {
	}

public:
	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

public:
	//! Immutable state of the positional delete
	shared_ptr<const IcebergPositionalDeleteData> data;
};

} // namespace duckdb
