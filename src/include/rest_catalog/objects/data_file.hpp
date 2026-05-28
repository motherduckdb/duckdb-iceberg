
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/content_file.hpp"
#include "rest_catalog/objects/count_map.hpp"
#include "rest_catalog/objects/value_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {



class DataFile {
public:
	DataFile();
	DataFile(const DataFile&) = delete;
	DataFile& operator=(const DataFile&) = delete;
	DataFile(DataFile&&) = default;
	DataFile &operator=(DataFile&&) = default;
public:
	static DataFile FromJSON(yyjson_val *obj);
	DataFile Copy() const;
public:
	string TryFromJSON(yyjson_val *obj);
public:
	ContentFile content_file;
	string content;
	int64_t first_row_id;
	bool has_first_row_id;
	CountMap column_sizes;
	bool has_column_sizes;
	CountMap value_counts;
	bool has_value_counts;
	CountMap null_value_counts;
	bool has_null_value_counts;
	CountMap nan_value_counts;
	bool has_nan_value_counts;
	ValueMap lower_bounds;
	bool has_lower_bounds;
	ValueMap upper_bounds;
	bool has_upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb

