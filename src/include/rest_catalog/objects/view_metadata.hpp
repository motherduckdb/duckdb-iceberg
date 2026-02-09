
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_history_entry.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewMetadata {
public:
	// Deserialization
	static ViewMetadata FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *val);

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	string view_uuid;
	int32_t format_version;
	string location;
	int32_t current_version_id;
	vector<ViewVersion> versions;
	vector<ViewHistoryEntry> version_log;
	vector<Schema> schemas;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
