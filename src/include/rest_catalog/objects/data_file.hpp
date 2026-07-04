
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
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
	DataFile(const DataFile &) = delete;
	DataFile &operator=(const DataFile &) = delete;
	DataFile(DataFile &&) = default;
	DataFile &operator=(DataFile &&) = default;

public:
	// Deserialization
	static DataFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	DataFile Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	ContentFile content_file;
	optional<int64_t> first_row_id;
	optional<CountMap> column_sizes;
	optional<CountMap> value_counts;
	optional<CountMap> null_value_counts;
	optional<CountMap> nan_value_counts;
	optional<ValueMap> lower_bounds;
	optional<ValueMap> upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb
