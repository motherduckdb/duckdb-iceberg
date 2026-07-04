
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/data_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class FileScanTask {
public:
	FileScanTask();
	FileScanTask(const FileScanTask &) = delete;
	FileScanTask &operator=(const FileScanTask &) = delete;
	FileScanTask(FileScanTask &&) = default;
	FileScanTask &operator=(FileScanTask &&) = default;

public:
	// Deserialization
	static FileScanTask FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FileScanTask Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	DataFile data_file;
	optional<vector<int32_t>> delete_file_references;
	unique_ptr<Expression> residual_filter;
};

} // namespace rest_api_objects
} // namespace duckdb
