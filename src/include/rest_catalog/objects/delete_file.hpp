
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/equality_delete_file.hpp"
#include "rest_catalog/objects/position_delete_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DeleteFile {
public:
	// Deserialization
	static DeleteFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	PositionDeleteFile position_delete_file;
	bool has_position_delete_file = false;
	EqualityDeleteFile equality_delete_file;
	bool has_equality_delete_file = false;
};

} // namespace rest_api_objects
} // namespace duckdb
