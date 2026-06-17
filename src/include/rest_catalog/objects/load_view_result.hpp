
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadViewResult {
public:
	LoadViewResult();
	LoadViewResult(const LoadViewResult &) = delete;
	LoadViewResult &operator=(const LoadViewResult &) = delete;
	LoadViewResult(LoadViewResult &&) = default;
	LoadViewResult &operator=(LoadViewResult &&) = default;

public:
	// Deserialization
	static LoadViewResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	LoadViewResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string metadata_location;
	ViewMetadata metadata;
	optional<case_insensitive_map_t<string>> config;
};

} // namespace rest_api_objects
} // namespace duckdb
