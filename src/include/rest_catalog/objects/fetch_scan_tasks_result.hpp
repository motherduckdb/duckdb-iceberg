
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksResult {
public:
	FetchScanTasksResult();
	FetchScanTasksResult(const FetchScanTasksResult &) = delete;
	FetchScanTasksResult &operator=(const FetchScanTasksResult &) = delete;
	FetchScanTasksResult(FetchScanTasksResult &&) = default;
	FetchScanTasksResult &operator=(FetchScanTasksResult &&) = default;

public:
	// Deserialization
	static FetchScanTasksResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FetchScanTasksResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	ScanTasks scan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
