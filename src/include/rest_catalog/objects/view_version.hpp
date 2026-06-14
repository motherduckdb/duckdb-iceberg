
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/view_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewVersion {
public:
	ViewVersion();
	ViewVersion(const ViewVersion &) = delete;
	ViewVersion &operator=(const ViewVersion &) = delete;
	ViewVersion(ViewVersion &&) = default;
	ViewVersion &operator=(ViewVersion &&) = default;

public:
	// Deserialization
	static ViewVersion FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ViewVersion Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int32_t version_id;
	int64_t timestamp_ms;
	int32_t schema_id;
	case_insensitive_map_t<string> summary;
	vector<ViewRepresentation> representations;
	Namespace default_namespace;
	string default_catalog;
	bool has_default_catalog = false;
};

} // namespace rest_api_objects
} // namespace duckdb
