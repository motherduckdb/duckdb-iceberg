
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/sqlview_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRepresentation {
public:
	// Deserialization
	static ViewRepresentation FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	SQLViewRepresentation sqlview_representation;
	bool has_sqlview_representation = false;
};

} // namespace rest_api_objects
} // namespace duckdb
