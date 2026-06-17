
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class EncryptedKey {
public:
	EncryptedKey();
	EncryptedKey(const EncryptedKey &) = delete;
	EncryptedKey &operator=(const EncryptedKey &) = delete;
	EncryptedKey(EncryptedKey &&) = default;
	EncryptedKey &operator=(EncryptedKey &&) = default;

public:
	// Deserialization
	static EncryptedKey FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	EncryptedKey Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string key_id;
	string encrypted_key_metadata;
	optional<string> encrypted_by_id;
	optional<case_insensitive_map_t<string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
