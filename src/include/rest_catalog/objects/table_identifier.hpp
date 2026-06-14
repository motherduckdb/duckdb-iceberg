
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
	TableIdentifier();
	TableIdentifier(const TableIdentifier &) = delete;
	TableIdentifier &operator=(const TableIdentifier &) = delete;
	TableIdentifier(TableIdentifier &&) = default;
	TableIdentifier &operator=(TableIdentifier &&) = default;

public:
	// Deserialization
	static TableIdentifier FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	TableIdentifier Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	Namespace _namespace;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
