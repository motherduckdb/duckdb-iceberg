
#include "rest_catalog/objects/add_view_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddViewVersionUpdate::AddViewVersionUpdate() {
}

AddViewVersionUpdate AddViewVersionUpdate::FromJSON(yyjson_val *obj) {
	AddViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddViewVersionUpdate AddViewVersionUpdate::Copy() const {
	AddViewVersionUpdate res;
	res.base_update = base_update.Copy();
	res.view_version = view_version.Copy();
	return res;
}

string AddViewVersionUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto view_version_val = yyjson_obj_get(obj, "view-version");
	if (!view_version_val) {
		return "AddViewVersionUpdate required property 'view-version' is missing";
	} else {
		error = view_version.TryFromJSON(view_version_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddViewVersionUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: view-version
	yyjson_mut_val *view_version_val = view_version.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "view-version", view_version_val);
}

yyjson_mut_val *AddViewVersionUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
