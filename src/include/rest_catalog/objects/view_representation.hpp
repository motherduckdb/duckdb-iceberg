
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/sqlview_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRepresentation {
public:
	ViewRepresentation();
	ViewRepresentation(const ViewRepresentation &) = delete;
	ViewRepresentation &operator=(const ViewRepresentation &) = delete;
	ViewRepresentation(ViewRepresentation &&) = default;
	ViewRepresentation &operator=(ViewRepresentation &&) = default;

public:
	// Deserialization
	static ViewRepresentation FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ViewRepresentation Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<SQLViewRepresentation> sqlview_representation;
};

} // namespace rest_api_objects
} // namespace duckdb
