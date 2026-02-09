
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionStatisticsFile {
public:
	// Deserialization
	static PartitionStatisticsFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
