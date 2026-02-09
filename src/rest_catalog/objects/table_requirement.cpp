
#include "rest_catalog/objects/table_requirement.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TableRequirement TableRequirement::FromJSON(yyjson_val *obj) {
	TableRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TableRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = assert_create.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_create = true;
			break;
		}
		error = assert_table_uuid.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_table_uuid = true;
			break;
		}
		error = assert_ref_snapshot_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_ref_snapshot_id = true;
			break;
		}
		error = assert_last_assigned_field_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_last_assigned_field_id = true;
			break;
		}
		error = assert_current_schema_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_current_schema_id = true;
			break;
		}
		error = assert_last_assigned_partition_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_last_assigned_partition_id = true;
			break;
		}
		error = assert_default_spec_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_default_spec_id = true;
			break;
		}
		error = assert_default_sort_order_id.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_default_sort_order_id = true;
			break;
		}
		return "TableRequirement failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *TableRequirement::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	if (has_assert_create) {
		return assert_create.ToJSON(doc);
	} else if (has_assert_table_uuid) {
		return assert_table_uuid.ToJSON(doc);
	} else if (has_assert_ref_snapshot_id) {
		return assert_ref_snapshot_id.ToJSON(doc);
	} else if (has_assert_last_assigned_field_id) {
		return assert_last_assigned_field_id.ToJSON(doc);
	} else if (has_assert_current_schema_id) {
		return assert_current_schema_id.ToJSON(doc);
	} else if (has_assert_last_assigned_partition_id) {
		return assert_last_assigned_partition_id.ToJSON(doc);
	} else if (has_assert_default_spec_id) {
		return assert_default_spec_id.ToJSON(doc);
	} else if (has_assert_default_sort_order_id) {
		return assert_default_sort_order_id.ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
