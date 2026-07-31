#include "planning/scan_plan/iceberg_scan_plan_state.hpp"

namespace duckdb {

IcebergScanPlanState::IcebergScanPlanState(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info_p,
                                           string path_p, const IcebergOptions &options_p)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), scan_info(std::move(scan_info_p)),
      path(std::move(path_p)), options(options_p) {
}

IcebergScanPlanState::~IcebergScanPlanState() {
	if (data_manifest_read_state) {
		//! FIXME: this could throw if the tasks encountered an error.
		data_manifest_read_state->executor.WorkOnTasks();
	}
}

} // namespace duckdb
