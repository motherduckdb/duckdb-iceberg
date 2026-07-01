
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	Type();
	Type(const Type &) = delete;
	Type &operator=(const Type &) = delete;
	Type(Type &&) = default;
	Type &operator=(Type &&) = default;

public:
	// Deserialization
	static Type FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	Type Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<PrimitiveType> primitive_type;
	optional<StructType> struct_type;
	optional<ListType> list_type;
	optional<MapType> map_type;
};

} // namespace rest_api_objects
} // namespace duckdb
