
#include "rest_catalog/objects/scan_tasks.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ScanTasks::ScanTasks() {
}

ScanTasks ScanTasks::FromJSON(yyjson_val *obj) {
	ScanTasks res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ScanTasks ScanTasks::Copy() const {
	ScanTasks res;
	if (has_delete_files) {
		res.delete_files.reserve(delete_files.size());
		for (auto &item : delete_files) {
			res.delete_files.emplace_back(item.Copy());
		}
	}
	res.has_delete_files = has_delete_files;
	if (has_file_scan_tasks) {
		res.file_scan_tasks.reserve(file_scan_tasks.size());
		for (auto &item : file_scan_tasks) {
			res.file_scan_tasks.emplace_back(item.Copy());
		}
	}
	res.has_file_scan_tasks = has_file_scan_tasks;
	if (has_plan_tasks) {
		res.plan_tasks.reserve(plan_tasks.size());
		for (auto &item : plan_tasks) {
			res.plan_tasks.emplace_back(item.Copy());
		}
	}
	res.has_plan_tasks = has_plan_tasks;
	return res;
}

string ScanTasks::TryFromJSON(yyjson_val *obj) {
	string error;
	auto delete_files_val = yyjson_obj_get(obj, "delete-files");
	if (delete_files_val && !yyjson_is_null(delete_files_val)) {
		has_delete_files = true;
		if (yyjson_is_arr(delete_files_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_files_val, idx, max, val) {
				DeleteFile tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				delete_files.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ScanTasks property 'delete_files' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(delete_files_val));
		}
	}
	auto file_scan_tasks_val = yyjson_obj_get(obj, "file-scan-tasks");
	if (file_scan_tasks_val && !yyjson_is_null(file_scan_tasks_val)) {
		has_file_scan_tasks = true;
		if (yyjson_is_arr(file_scan_tasks_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(file_scan_tasks_val, idx, max, val) {
				FileScanTask tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				file_scan_tasks.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ScanTasks property 'file_scan_tasks' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(file_scan_tasks_val));
		}
	}
	auto plan_tasks_val = yyjson_obj_get(obj, "plan-tasks");
	if (plan_tasks_val && !yyjson_is_null(plan_tasks_val)) {
		has_plan_tasks = true;
		if (yyjson_is_arr(plan_tasks_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(plan_tasks_val, idx, max, val) {
				PlanTask tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				plan_tasks.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ScanTasks property 'plan_tasks' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(plan_tasks_val));
		}
	}
	return "";
}

void ScanTasks::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: delete-files
	if (has_delete_files) {
		yyjson_mut_val *delete_files_arr = yyjson_mut_arr(doc);
		for (const auto &item : delete_files) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(delete_files_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "delete-files", delete_files_arr);
	}

	// Serialize: file-scan-tasks
	if (has_file_scan_tasks) {
		yyjson_mut_val *file_scan_tasks_arr = yyjson_mut_arr(doc);
		for (const auto &item : file_scan_tasks) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(file_scan_tasks_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "file-scan-tasks", file_scan_tasks_arr);
	}

	// Serialize: plan-tasks
	if (has_plan_tasks) {
		yyjson_mut_val *plan_tasks_arr = yyjson_mut_arr(doc);
		for (const auto &item : plan_tasks) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(plan_tasks_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "plan-tasks", plan_tasks_arr);
	}
}

yyjson_mut_val *ScanTasks::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
