
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/binary_type_value.hpp"
#include "rest_catalog/objects/file_format.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ContentFile {
public:
	ContentFile();
	ContentFile(const ContentFile &) = delete;
	ContentFile &operator=(const ContentFile &) = delete;
	ContentFile(ContentFile &&) = default;
	ContentFile &operator=(ContentFile &&) = default;

public:
	// Deserialization
	static ContentFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ContentFile Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t spec_id;
	vector<PrimitiveTypeValue> partition;
	string content;
	string file_path;
	FileFormat file_format;
	int64_t file_size_in_bytes;
	int64_t record_count;
	BinaryTypeValue key_metadata;
	bool has_key_metadata = false;
	vector<int64_t> split_offsets;
	bool has_split_offsets = false;
	int32_t sort_order_id;
	bool has_sort_order_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
