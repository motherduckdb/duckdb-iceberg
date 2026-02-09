
#include "rest_catalog/objects/fetch_scan_tasks_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksResult FetchScanTasksResult::FromJSON(yyjson_val *obj) {
	FetchScanTasksResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FetchScanTasksResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

yyjson_mut_val *FetchScanTasksResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: ScanTasks
	yyjson_mut_val *scan_tasksbase_obj = scan_tasks.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(scan_tasksbase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
