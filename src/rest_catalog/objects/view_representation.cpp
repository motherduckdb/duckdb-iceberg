
#include "rest_catalog/objects/view_representation.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewRepresentation::ViewRepresentation() {
}

ViewRepresentation ViewRepresentation::FromJSON(yyjson_val *obj) {
	ViewRepresentation res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ViewRepresentation ViewRepresentation::Copy() const {
	ViewRepresentation res;
	if (sqlview_representation.has_value()) {
		res.sqlview_representation.emplace();
		(*res.sqlview_representation) = (*sqlview_representation).Copy();
	}
	return res;
}

string ViewRepresentation::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		sqlview_representation.emplace();
		error = sqlview_representation->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			sqlview_representation = nullopt;
		}
		return "ViewRepresentation failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

void ViewRepresentation::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (sqlview_representation.has_value()) {
		sqlview_representation->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *ViewRepresentation::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
