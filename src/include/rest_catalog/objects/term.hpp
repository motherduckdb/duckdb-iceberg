
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform_term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	// Deserialization
	static Term FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	Reference reference;
	bool has_reference = false;
	TransformTerm transform_term;
	bool has_transform_term = false;
};

} // namespace rest_api_objects
} // namespace duckdb
