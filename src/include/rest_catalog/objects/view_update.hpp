
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_view_version_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/set_current_view_version_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewUpdate {
public:
	ViewUpdate();
	ViewUpdate(const ViewUpdate &) = delete;
	ViewUpdate &operator=(const ViewUpdate &) = delete;
	ViewUpdate(ViewUpdate &&) = default;
	ViewUpdate &operator=(ViewUpdate &&) = default;

public:
	// Deserialization
	static ViewUpdate FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ViewUpdate Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<AssignUUIDUpdate> assign_uuidupdate;
	optional<UpgradeFormatVersionUpdate> upgrade_format_version_update;
	optional<AddSchemaUpdate> add_schema_update;
	optional<SetLocationUpdate> set_location_update;
	optional<SetPropertiesUpdate> set_properties_update;
	optional<RemovePropertiesUpdate> remove_properties_update;
	optional<AddViewVersionUpdate> add_view_version_update;
	optional<SetCurrentViewVersionUpdate> set_current_view_version_update;
};

} // namespace rest_api_objects
} // namespace duckdb
