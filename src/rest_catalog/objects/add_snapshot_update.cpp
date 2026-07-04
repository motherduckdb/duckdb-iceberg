
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddSnapshotUpdate::AddSnapshotUpdate() {
}

AddSnapshotUpdate AddSnapshotUpdate::FromJSON(yyjson_val *obj) {
	AddSnapshotUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSnapshotUpdate AddSnapshotUpdate::Copy() const {
	AddSnapshotUpdate res;
	res.base_update = base_update.Copy();
	res.snapshot = snapshot.Copy();
	return res;
}

string AddSnapshotUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto snapshot_val = yyjson_obj_get(obj, "snapshot");
	if (!snapshot_val) {
		return "AddSnapshotUpdate required property 'snapshot' is missing";
	} else {
		error = snapshot.TryFromJSON(snapshot_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddSnapshotUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: snapshot
	yyjson_mut_val *snapshot_val = snapshot.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "snapshot", snapshot_val);
}

yyjson_mut_val *AddSnapshotUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
