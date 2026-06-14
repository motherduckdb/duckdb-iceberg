
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/metrics.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitReport {
public:
	CommitReport();
	CommitReport(const CommitReport &) = delete;
	CommitReport &operator=(const CommitReport &) = delete;
	CommitReport(CommitReport &&) = default;
	CommitReport &operator=(CommitReport &&) = default;

public:
	// Deserialization
	static CommitReport FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CommitReport Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string table_name;
	int64_t snapshot_id;
	int64_t sequence_number;
	string operation;
	Metrics metrics;
	case_insensitive_map_t<string> metadata;
	bool has_metadata = false;
};

} // namespace rest_api_objects
} // namespace duckdb
