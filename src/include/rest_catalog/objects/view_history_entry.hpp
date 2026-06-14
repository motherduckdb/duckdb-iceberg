
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewHistoryEntry {
public:
	ViewHistoryEntry();
	ViewHistoryEntry(const ViewHistoryEntry &) = delete;
	ViewHistoryEntry &operator=(const ViewHistoryEntry &) = delete;
	ViewHistoryEntry(ViewHistoryEntry &&) = default;
	ViewHistoryEntry &operator=(ViewHistoryEntry &&) = default;

public:
	// Deserialization
	static ViewHistoryEntry FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ViewHistoryEntry Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t version_id;
	int64_t timestamp_ms;
};

} // namespace rest_api_objects
} // namespace duckdb
