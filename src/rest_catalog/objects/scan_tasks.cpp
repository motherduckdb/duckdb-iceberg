
#include "rest_catalog/objects/scan_tasks.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ScanTasks::ScanTasks() {
}

ScanTasks ScanTasks::FromJSON(JSONValue obj) {
	ScanTasks res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ScanTasks ScanTasks::Copy() const {
	ScanTasks res;
	if (delete_files.has_value()) {
		res.delete_files.emplace();
		(*res.delete_files).reserve((*delete_files).size());
		for (auto &item : (*delete_files)) {
			(*res.delete_files).emplace_back(item.Copy());
		}
	}
	if (file_scan_tasks.has_value()) {
		res.file_scan_tasks.emplace();
		(*res.file_scan_tasks).reserve((*file_scan_tasks).size());
		for (auto &item : (*file_scan_tasks)) {
			(*res.file_scan_tasks).emplace_back(item.Copy());
		}
	}
	if (plan_tasks.has_value()) {
		res.plan_tasks.emplace();
		(*res.plan_tasks).reserve((*plan_tasks).size());
		for (auto &item : (*plan_tasks)) {
			(*res.plan_tasks).emplace_back(item.Copy());
		}
	}
	return res;
}

string ScanTasks::TryFromJSON(JSONValue obj) {
	string error;
	auto delete_files_val = obj.GetMember("delete-files");
	if (delete_files_val.IsValid()) {
		vector<DeleteFile> delete_files_tmp;
		if (delete_files_val.IsArray()) {
			delete_files_val.IterateArray([&](JSONValue delete_files_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				DeleteFile delete_files_tmp_item;
				error = delete_files_tmp_item.TryFromJSON(delete_files_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				delete_files_tmp.emplace_back(std::move(delete_files_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ScanTasks property 'delete_files_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(delete_files_val).c_str());
		}
		delete_files = std::move(delete_files_tmp);
	}
	auto file_scan_tasks_val = obj.GetMember("file-scan-tasks");
	if (file_scan_tasks_val.IsValid()) {
		vector<FileScanTask> file_scan_tasks_tmp;
		if (file_scan_tasks_val.IsArray()) {
			file_scan_tasks_val.IterateArray([&](JSONValue file_scan_tasks_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				FileScanTask file_scan_tasks_tmp_item;
				error = file_scan_tasks_tmp_item.TryFromJSON(file_scan_tasks_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				file_scan_tasks_tmp.emplace_back(std::move(file_scan_tasks_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanTasks property 'file_scan_tasks_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(file_scan_tasks_val).c_str());
		}
		file_scan_tasks = std::move(file_scan_tasks_tmp);
	}
	auto plan_tasks_val = obj.GetMember("plan-tasks");
	if (plan_tasks_val.IsValid()) {
		vector<PlanTask> plan_tasks_tmp;
		if (plan_tasks_val.IsArray()) {
			plan_tasks_val.IterateArray([&](JSONValue plan_tasks_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PlanTask plan_tasks_tmp_item;
				error = plan_tasks_tmp_item.TryFromJSON(plan_tasks_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				plan_tasks_tmp.emplace_back(std::move(plan_tasks_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ScanTasks property 'plan_tasks_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(plan_tasks_val).c_str());
		}
		plan_tasks = std::move(plan_tasks_tmp);
	}
	return "";
}

void ScanTasks::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: delete-files
	if (delete_files.has_value()) {
		auto &delete_files_value = *delete_files;
		auto delete_files_value_arr = writer.CreateArray();
		for (const auto &item : delete_files_value) {
			auto item_val = item.ToJSON(writer);
			delete_files_value_arr.Append(item_val);
		}
		obj.Add("delete-files", delete_files_value_arr);
	}

	// Serialize: file-scan-tasks
	if (file_scan_tasks.has_value()) {
		auto &file_scan_tasks_value = *file_scan_tasks;
		auto file_scan_tasks_value_arr = writer.CreateArray();
		for (const auto &item : file_scan_tasks_value) {
			auto item_val = item.ToJSON(writer);
			file_scan_tasks_value_arr.Append(item_val);
		}
		obj.Add("file-scan-tasks", file_scan_tasks_value_arr);
	}

	// Serialize: plan-tasks
	if (plan_tasks.has_value()) {
		auto &plan_tasks_value = *plan_tasks;
		auto plan_tasks_value_arr = writer.CreateArray();
		for (const auto &item : plan_tasks_value) {
			auto item_val = item.ToJSON(writer);
			plan_tasks_value_arr.Append(item_val);
		}
		obj.Add("plan-tasks", plan_tasks_value_arr);
	}
}

JSONMutableValue ScanTasks::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
