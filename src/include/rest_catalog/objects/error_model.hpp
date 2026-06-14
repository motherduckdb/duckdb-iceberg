
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	ErrorModel();
	ErrorModel(const ErrorModel &) = delete;
	ErrorModel &operator=(const ErrorModel &) = delete;
	ErrorModel(ErrorModel &&) = default;
	ErrorModel &operator=(ErrorModel &&) = default;

public:
	// Deserialization
	static ErrorModel FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ErrorModel Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string message;
	string type;
	int32_t code;
	optional<vector<string>> stack;
};

} // namespace rest_api_objects
} // namespace duckdb
