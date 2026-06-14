
#include "rest_catalog/objects/remove_snapshot_ref_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoveSnapshotRefUpdate::RemoveSnapshotRefUpdate() {
}

RemoveSnapshotRefUpdate RemoveSnapshotRefUpdate::FromJSON(yyjson_val *obj) {
	RemoveSnapshotRefUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemoveSnapshotRefUpdate RemoveSnapshotRefUpdate::Copy() const {
	RemoveSnapshotRefUpdate res;
	res.base_update = base_update.Copy();
	res.ref_name = ref_name;
	return res;
}

string RemoveSnapshotRefUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto ref_name_val = yyjson_obj_get(obj, "ref-name");
	if (!ref_name_val) {
		return "RemoveSnapshotRefUpdate required property 'ref-name' is missing";
	} else {
		if (yyjson_is_str(ref_name_val)) {
			ref_name = yyjson_get_str(ref_name_val);
		} else {
			return StringUtil::Format(
			    "RemoveSnapshotRefUpdate property 'ref_name' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(ref_name_val));
		}
	}
	return "";
}

void RemoveSnapshotRefUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: ref-name
	yyjson_mut_obj_add_strcpy(doc, obj, "ref-name", ref_name.c_str());
}

yyjson_mut_val *RemoveSnapshotRefUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
