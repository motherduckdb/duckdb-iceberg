//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/statistics/iceberg_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

#pragma once

namespace duckdb {

struct IcebergColumnStats {
	explicit IcebergColumnStats(LogicalType type_p) : type(std::move(type_p)) {
	}

	// Copy constructor
	IcebergColumnStats(const IcebergColumnStats &other);
	IcebergColumnStats &operator=(const IcebergColumnStats &other);
	IcebergColumnStats(IcebergColumnStats &&other) noexcept = default;
	IcebergColumnStats &operator=(IcebergColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t num_values = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_num_values = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;
	bool has_column_size_bytes = false;

	// Geometry bounding-box stats produced by the parquet writer's RETURN_STATS.
	// Z/M halves are only set when the parquet writer emitted them, which it does
	// iff the data actually contained those dimensions.
	bool has_bbox_xy = false;
	double bbox_xmin = 0.0;
	double bbox_xmax = 0.0;
	double bbox_ymin = 0.0;
	double bbox_ymax = 0.0;
	bool has_bbox_z = false;
	double bbox_zmin = 0.0;
	double bbox_zmax = 0.0;
	bool has_bbox_m = false;
	double bbox_mmin = 0.0;
	double bbox_mmax = 0.0;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const IcebergColumnStats &new_stats);
	IcebergColumnStats Copy() const;
	static IcebergColumnStats ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats,
	                                           ClientContext &context);

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

} // namespace duckdb
