
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FileFormat {
public:
	FileFormat();
	FileFormat(const FileFormat &) = delete;
	FileFormat &operator=(const FileFormat &) = delete;
	FileFormat(FileFormat &&) = default;
	FileFormat &operator=(FileFormat &&) = default;

public:
	// Deserialization
	static FileFormat FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	FileFormat Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
