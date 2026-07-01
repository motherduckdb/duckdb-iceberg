
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesRequest {
public:
	UpdateNamespacePropertiesRequest();
	UpdateNamespacePropertiesRequest(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest &operator=(const UpdateNamespacePropertiesRequest &) = delete;
	UpdateNamespacePropertiesRequest(UpdateNamespacePropertiesRequest &&) = default;
	UpdateNamespacePropertiesRequest &operator=(UpdateNamespacePropertiesRequest &&) = default;

public:
	// Deserialization
	static UpdateNamespacePropertiesRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	UpdateNamespacePropertiesRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<vector<string>> removals;
	optional<case_insensitive_map_t<string>> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
