
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StorageCredential {
public:
	StorageCredential();
	StorageCredential(const StorageCredential &) = delete;
	StorageCredential &operator=(const StorageCredential &) = delete;
	StorageCredential(StorageCredential &&) = default;
	StorageCredential &operator=(StorageCredential &&) = default;

public:
	// Deserialization
	static StorageCredential FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	StorageCredential Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string prefix;
	case_insensitive_map_t<string> config;
};

} // namespace rest_api_objects
} // namespace duckdb
