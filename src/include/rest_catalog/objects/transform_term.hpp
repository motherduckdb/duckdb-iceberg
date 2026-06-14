
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TransformTerm {
public:
	TransformTerm();
	TransformTerm(const TransformTerm &) = delete;
	TransformTerm &operator=(const TransformTerm &) = delete;
	TransformTerm(TransformTerm &&) = default;
	TransformTerm &operator=(TransformTerm &&) = default;

public:
	// Deserialization
	static TransformTerm FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	TransformTerm Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	Transform transform;
	Reference term;
};

} // namespace rest_api_objects
} // namespace duckdb
