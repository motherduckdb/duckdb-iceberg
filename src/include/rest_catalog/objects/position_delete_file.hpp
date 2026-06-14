
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PositionDeleteFile {
public:
	PositionDeleteFile();
	PositionDeleteFile(const PositionDeleteFile &) = delete;
	PositionDeleteFile &operator=(const PositionDeleteFile &) = delete;
	PositionDeleteFile(PositionDeleteFile &&) = default;
	PositionDeleteFile &operator=(PositionDeleteFile &&) = default;

public:
	// Deserialization
	static PositionDeleteFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PositionDeleteFile Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	ContentFile content_file;
	int64_t content_offset;
	bool has_content_offset = false;
	int64_t content_size_in_bytes;
	bool has_content_size_in_bytes = false;
};

} // namespace rest_api_objects
} // namespace duckdb
