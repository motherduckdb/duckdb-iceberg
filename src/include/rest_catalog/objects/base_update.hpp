
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BaseUpdate {
public:
	BaseUpdate();
	BaseUpdate(const BaseUpdate &) = delete;
	BaseUpdate &operator=(const BaseUpdate &) = delete;
	BaseUpdate(BaseUpdate &&) = default;
	BaseUpdate &operator=(BaseUpdate &&) = default;

public:
	// Deserialization
	static BaseUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	BaseUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
