
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class ListType {
public:
	ListType();
	ListType(const ListType &) = delete;
	ListType &operator=(const ListType &) = delete;
	ListType(ListType &&) = default;
	ListType &operator=(ListType &&) = default;

public:
	// Deserialization
	static ListType FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ListType Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	int32_t element_id;
	unique_ptr<Type> element;
	bool element_required;
};

} // namespace rest_api_objects
} // namespace duckdb
