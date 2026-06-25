
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetDefaultSpecUpdate {
public:
	SetDefaultSpecUpdate();
	SetDefaultSpecUpdate(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate &operator=(const SetDefaultSpecUpdate &) = delete;
	SetDefaultSpecUpdate(SetDefaultSpecUpdate &&) = default;
	SetDefaultSpecUpdate &operator=(SetDefaultSpecUpdate &&) = default;

public:
	// Deserialization
	static SetDefaultSpecUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	SetDefaultSpecUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	BaseUpdate base_update;
	int32_t spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
