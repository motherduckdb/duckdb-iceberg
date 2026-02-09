
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/assert_view_uuid.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRequirement {
public:
	// Deserialization
	static ViewRequirement FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	AssertViewUUID assert_view_uuid;
	bool has_assert_view_uuid = false;
};

} // namespace rest_api_objects
} // namespace duckdb
