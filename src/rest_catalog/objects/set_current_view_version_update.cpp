
#include "rest_catalog/objects/set_current_view_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetCurrentViewVersionUpdate::SetCurrentViewVersionUpdate() {
}

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::FromJSON(yyjson_val *obj) {
	SetCurrentViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::Copy() const {
	SetCurrentViewVersionUpdate res;
	res.base_update = base_update.Copy();
	res.view_version_id = view_version_id;
	return res;
}

string SetCurrentViewVersionUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto view_version_id_val = yyjson_obj_get(obj, "view-version-id");
	if (!view_version_id_val) {
		return "SetCurrentViewVersionUpdate required property 'view-version-id' is missing";
	} else {
		if (yyjson_is_int(view_version_id_val)) {
			view_version_id = yyjson_get_int(view_version_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentViewVersionUpdate property 'view_version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(view_version_id_val));
		}
	}
	return "";
}

void SetCurrentViewVersionUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: view-version-id
	yyjson_mut_obj_add_int(doc, obj, "view-version-id", view_version_id);
}

yyjson_mut_val *SetCurrentViewVersionUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
