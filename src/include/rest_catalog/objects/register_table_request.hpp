
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RegisterTableRequest {
public:
	RegisterTableRequest();
	RegisterTableRequest(const RegisterTableRequest &) = delete;
	RegisterTableRequest &operator=(const RegisterTableRequest &) = delete;
	RegisterTableRequest(RegisterTableRequest &&) = default;
	RegisterTableRequest &operator=(RegisterTableRequest &&) = default;

public:
	// Deserialization
	static RegisterTableRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RegisterTableRequest Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string name;
	string metadata_location;
	bool overwrite;
	bool has_overwrite = false;
};

} // namespace rest_api_objects
} // namespace duckdb
