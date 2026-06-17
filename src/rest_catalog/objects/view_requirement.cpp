
#include "rest_catalog/objects/view_requirement.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewRequirement::ViewRequirement() {
}

ViewRequirement ViewRequirement::FromJSON(yyjson_val *obj) {
	ViewRequirement res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewRequirement ViewRequirement::Copy() const {
	ViewRequirement res;
	if (assert_view_uuid.has_value()) {
		res.assert_view_uuid.emplace();
		(*res.assert_view_uuid) = (*assert_view_uuid).Copy();
	}
	return res;
}

string ViewRequirement::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		assert_view_uuid.emplace();
		error = assert_view_uuid->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			assert_view_uuid = nullopt;
		}
		return "ViewRequirement failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void ViewRequirement::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (assert_view_uuid.has_value()) {
		assert_view_uuid->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *ViewRequirement::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
