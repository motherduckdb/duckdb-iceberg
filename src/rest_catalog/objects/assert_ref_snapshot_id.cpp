
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertRefSnapshotId::AssertRefSnapshotId() {
}

AssertRefSnapshotId AssertRefSnapshotId::FromJSON(yyjson_val *obj) {
	AssertRefSnapshotId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertRefSnapshotId AssertRefSnapshotId::Copy() const {
	AssertRefSnapshotId res;
	res.table_requirement = table_requirement.Copy();
	res.ref = ref;
	res.snapshot_id = snapshot_id;
	return res;
}

string AssertRefSnapshotId::TryFromJSON(yyjson_val *obj) {
	string error;
	error = table_requirement.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto ref_val = yyjson_obj_get(obj, "ref");
	if (!ref_val) {
		return "AssertRefSnapshotId required property 'ref' is missing";
	} else {
		if (yyjson_is_str(ref_val)) {
			ref = yyjson_get_str(ref_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'ref' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(ref_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "AssertRefSnapshotId required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_null(snapshot_id_val)) {
			//! do nothing, property is explicitly nullable
		} else if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "AssertRefSnapshotId property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	return "";
}

void AssertRefSnapshotId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: TableRequirement
	table_requirement.PopulateJSON(doc, obj);

	// Serialize: ref
	yyjson_mut_obj_add_strcpy(doc, obj, "ref", ref.c_str());

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);
}

yyjson_mut_val *AssertRefSnapshotId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
