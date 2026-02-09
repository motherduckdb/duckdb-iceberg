
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/null_order.hpp"
#include "rest_catalog/objects/sort_direction.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortField {
public:
	// Deserialization
	static SortField FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	int32_t source_id;
	Transform transform;
	SortDirection direction;
	NullOrder null_order;
};

} // namespace rest_api_objects
} // namespace duckdb
