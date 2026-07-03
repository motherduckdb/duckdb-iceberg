
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoveEncryptionKeyUpdate {
public:
	RemoveEncryptionKeyUpdate();
	RemoveEncryptionKeyUpdate(const RemoveEncryptionKeyUpdate &) = delete;
	RemoveEncryptionKeyUpdate &operator=(const RemoveEncryptionKeyUpdate &) = delete;
	RemoveEncryptionKeyUpdate(RemoveEncryptionKeyUpdate &&) = default;
	RemoveEncryptionKeyUpdate &operator=(RemoveEncryptionKeyUpdate &&) = default;

public:
	// Deserialization
	static RemoveEncryptionKeyUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemoveEncryptionKeyUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	string key_id;
};

} // namespace rest_api_objects
} // namespace duckdb
