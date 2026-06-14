
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/storage_credential.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadTableResult {
public:
	LoadTableResult();
	LoadTableResult(const LoadTableResult &) = delete;
	LoadTableResult &operator=(const LoadTableResult &) = delete;
	LoadTableResult(LoadTableResult &&) = default;
	LoadTableResult &operator=(LoadTableResult &&) = default;

public:
	// Deserialization
	static LoadTableResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	LoadTableResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	TableMetadata metadata;
	string metadata_location;
	bool has_metadata_location = false;
	case_insensitive_map_t<string> config;
	bool has_config = false;
	vector<StorageCredential> storage_credentials;
	bool has_storage_credentials = false;
};

} // namespace rest_api_objects
} // namespace duckdb
