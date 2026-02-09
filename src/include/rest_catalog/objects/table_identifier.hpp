
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableIdentifier {
public:
	// Deserialization
	static TableIdentifier FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	Namespace _namespace;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
