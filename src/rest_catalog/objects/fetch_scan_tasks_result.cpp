
#include "rest_catalog/objects/fetch_scan_tasks_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksResult::FetchScanTasksResult() {
}

FetchScanTasksResult FetchScanTasksResult::FromJSON(yyjson_val *obj) {
	FetchScanTasksResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FetchScanTasksResult FetchScanTasksResult::Copy() const {
	FetchScanTasksResult res;
	res.scan_tasks = scan_tasks.Copy();
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

void FetchScanTasksResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: ScanTasks
	scan_tasks.PopulateJSON(doc, obj);
}

yyjson_mut_val *FetchScanTasksResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
