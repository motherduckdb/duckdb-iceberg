#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/scan_plan/iceberg_manifest_store.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"

namespace duckdb {

IcebergScanPlanState::IcebergScanPlanState(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info_p,
                                           string path_p, const IcebergOptions &options_p)
    : context(context_p),
      fs(FileSystem::GetFileSystem(context)), configuration {std::move(scan_info_p), std::move(path_p), nullptr,
                                                             options_p, true} {
}

const IcebergScanConfiguration &IcebergScanPlanState::Configuration() const {
	return configuration;
}

void IcebergScanPlanState::RequireConfigurable() const {
	if (configuration_frozen || scan_identity_frozen) {
		throw InternalException("Cannot change Iceberg scan configuration after planning or view creation has started");
	}
}

void IcebergScanPlanState::RequirePlanningConfigurable() const {
	if (configuration_frozen) {
		throw InternalException("Cannot change Iceberg scan configuration after planning or view creation has started");
	}
}

void IcebergScanPlanState::SetScanInfo(shared_ptr<IcebergScanInfo> scan_info) {
	RequireConfigurable();
	configuration.scan_info = std::move(scan_info);
}

void IcebergScanPlanState::SetOptions(const IcebergOptions &options) {
	RequireConfigurable();
	configuration.options = options;
}

void IcebergScanPlanState::SetTable(IcebergTableSchemaVersion &table) {
	RequireConfigurable();
	configuration.table = table;
}

void IcebergScanPlanState::DisableServerSidePlanning() {
	RequirePlanningConfigurable();
	configuration.server_side_planning_enabled = false;
}

void IcebergScanPlanState::RequireRowIds() {
	RequirePlanningConfigurable();
	configuration.row_ids_required = true;
}

void IcebergScanPlanState::FreezeScanIdentity() {
	if (!configuration.scan_info) {
		throw InternalException("Cannot start Iceberg scan planning without scan metadata");
	}
	scan_identity_frozen = true;
}

void IcebergScanPlanState::FreezeConfiguration() {
	FreezeScanIdentity();
	configuration_frozen = true;
}

IcebergScanPlanState::~IcebergScanPlanState() = default;

IcebergEqualityDeleteBounds &IcebergScanPlanState::GetEqualityDeleteBounds(idx_t manifest_idx, idx_t entry_idx) {
	lock_guard<mutex> guard(equality_bounds_lock);
	auto &bounds = equality_delete_bounds[manifest_idx][entry_idx];
	if (!bounds) {
		bounds = make_uniq<IcebergEqualityDeleteBounds>();
	}
	return *bounds;
}

IcebergScanPlanProvider &IcebergScanPlanState::GetScanPlanProvider(const IcebergTableFilters &filters,
                                                                   const IcebergScanOrder &scan_order) {
	if (!scan_plan_provider) {
		FreezeConfiguration();
		scan_plan_provider = IcebergScanPlanProvider::Create(*this, CreatePlanContext(), configuration.table, filters,
		                                                     scan_order, configuration.server_side_planning_enabled);
	}
	return *scan_plan_provider;
}

bool IcebergScanPlanState::HasScanPlanProvider() const {
	return scan_plan_provider != nullptr;
}

bool IcebergScanPlanState::MayPlanServerSide() const {
	auto &scan_info = *configuration.scan_info;
	return IcebergScanPlanProvider::MayPlanServerSide(context, scan_info.snapshot_info, configuration.table,
	                                                  scan_info.transaction_data != nullptr,
	                                                  configuration.server_side_planning_enabled);
}

IcebergScanPlanContext IcebergScanPlanState::CreatePlanContext() const {
	if (!configuration.scan_info) {
		throw InternalException("Cannot start Iceberg scan planning without scan metadata");
	}
	auto &scan_info = *configuration.scan_info;
	return IcebergScanPlanContext {context,
	                               fs,
	                               configuration.path,
	                               configuration.options,
	                               scan_info.snapshot_info,
	                               scan_info.metadata,
	                               scan_info.schema,
	                               scan_info.transaction_data.get(),
	                               scan_info.file_secret};
}

IcebergManifestStore &IcebergScanPlanState::GetManifestStore(IcebergScanPlanContext context) {
	FreezeScanIdentity();
	if (!manifest_store) {
		manifest_store = make_uniq<IcebergManifestStore>(lock, std::move(context));
	}
	return *manifest_store;
}

} // namespace duckdb
