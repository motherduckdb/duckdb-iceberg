
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadCredentialsResponse {
public:
	LoadCredentialsResponse();
	LoadCredentialsResponse(const LoadCredentialsResponse &) = delete;
	LoadCredentialsResponse &operator=(const LoadCredentialsResponse &) = delete;
	LoadCredentialsResponse(LoadCredentialsResponse &&) = default;
	LoadCredentialsResponse &operator=(LoadCredentialsResponse &&) = default;

public:
	// Deserialization
	static LoadCredentialsResponse FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	LoadCredentialsResponse Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
