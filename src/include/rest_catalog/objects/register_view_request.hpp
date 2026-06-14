
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RegisterViewRequest {
public:
	RegisterViewRequest();
	RegisterViewRequest(const RegisterViewRequest &) = delete;
	RegisterViewRequest &operator=(const RegisterViewRequest &) = delete;
	RegisterViewRequest(RegisterViewRequest &&) = default;
	RegisterViewRequest &operator=(RegisterViewRequest &&) = default;

public:
	// Deserialization
	static RegisterViewRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RegisterViewRequest Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string name;
	string metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
