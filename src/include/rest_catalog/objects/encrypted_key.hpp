
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class EncryptedKey {
public:
	// Deserialization
	static EncryptedKey FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string key_id;
	string encrypted_key_metadata;
	string encrypted_by_id;
	bool has_encrypted_by_id = false;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
