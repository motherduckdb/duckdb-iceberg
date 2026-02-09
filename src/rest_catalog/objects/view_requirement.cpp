
#include "rest_catalog/objects/view_requirement.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewRequirement ViewRequirement::FromJSON(yyjson_val *obj) {
	ViewRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ViewRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = assert_view_uuid.TryFromJSON(obj);
		if (error.empty()) {
			has_assert_view_uuid = true;
			break;
		}
		return "ViewRequirement failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *ViewRequirement::ToJSON(yyjson_mut_doc *doc) const {
	if (has_assert_view_uuid) {
		return assert_view_uuid.ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
