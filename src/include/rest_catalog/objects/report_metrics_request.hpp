
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/commit_report.hpp"
#include "rest_catalog/objects/scan_report.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ReportMetricsRequest {
public:
	ReportMetricsRequest();
	ReportMetricsRequest(const ReportMetricsRequest &) = delete;
	ReportMetricsRequest &operator=(const ReportMetricsRequest &) = delete;
	ReportMetricsRequest(ReportMetricsRequest &&) = default;
	ReportMetricsRequest &operator=(ReportMetricsRequest &&) = default;

public:
	// Deserialization
	static ReportMetricsRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ReportMetricsRequest Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	ScanReport scan_report;
	bool has_scan_report = false;
	CommitReport commit_report;
	bool has_commit_report = false;
	string report_type;
};

} // namespace rest_api_objects
} // namespace duckdb
