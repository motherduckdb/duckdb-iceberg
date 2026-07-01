
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/delete_file.hpp"
#include "rest_catalog/objects/file_scan_task.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ScanTasks {
public:
	ScanTasks();
	ScanTasks(const ScanTasks &) = delete;
	ScanTasks &operator=(const ScanTasks &) = delete;
	ScanTasks(ScanTasks &&) = default;
	ScanTasks &operator=(ScanTasks &&) = default;

public:
	// Deserialization
	static ScanTasks FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ScanTasks Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<vector<DeleteFile>> delete_files;
	optional<vector<FileScanTask>> file_scan_tasks;
	optional<vector<PlanTask>> plan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
