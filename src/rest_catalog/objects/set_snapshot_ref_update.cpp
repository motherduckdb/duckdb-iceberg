
#include "rest_catalog/objects/set_snapshot_ref_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetSnapshotRefUpdate::SetSnapshotRefUpdate() {
}

SetSnapshotRefUpdate SetSnapshotRefUpdate::FromJSON(yyjson_val *obj) {
	SetSnapshotRefUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetSnapshotRefUpdate SetSnapshotRefUpdate::Copy() const {
	SetSnapshotRefUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot_reference = snapshot_reference.Copy();
	res.ref_name = ref_name;
	if (has_action) {
		res.action = action;
	}
	res.has_action = has_action;
	return res;
}

string SetSnapshotRefUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = snapshot_reference.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto ref_name_val = yyjson_obj_get(obj, "ref-name");
	if (!ref_name_val) {
		return "SetSnapshotRefUpdate required property 'ref-name' is missing";
	} else {
		if (yyjson_is_str(ref_name_val)) {
			ref_name = yyjson_get_str(ref_name_val);
		} else {
			return StringUtil::Format(
			    "SetSnapshotRefUpdate property 'ref_name' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(ref_name_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val && !yyjson_is_null(action_val)) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetSnapshotRefUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return "";
}

yyjson_mut_val *SetSnapshotRefUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: BaseUpdate
	yyjson_mut_val *base_updatebase_obj = base_update.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(base_updatebase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize base class: SnapshotReference
	yyjson_mut_val *snapshot_referencebase_obj = snapshot_reference.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(snapshot_referencebase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize: ref-name
	yyjson_mut_obj_add_str(doc, obj, "ref-name", ref_name.c_str());

	// Serialize: action
	if (has_action) {
		yyjson_mut_obj_add_str(doc, obj, "action", action.c_str());
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
