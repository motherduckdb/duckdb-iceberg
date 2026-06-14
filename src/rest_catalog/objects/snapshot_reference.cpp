
#include "rest_catalog/objects/snapshot_reference.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SnapshotReference::SnapshotReference() {
}

SnapshotReference SnapshotReference::FromJSON(yyjson_val *obj) {
	SnapshotReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotReference SnapshotReference::Copy() const {
	SnapshotReference res;
	res.type = type;
	res.snapshot_id = snapshot_id;
	if (max_ref_age_ms.has_value()) {
		res.max_ref_age_ms.emplace();
		(*res.max_ref_age_ms) = (*max_ref_age_ms);
	}
	if (max_snapshot_age_ms.has_value()) {
		res.max_snapshot_age_ms.emplace();
		(*res.max_snapshot_age_ms) = (*max_snapshot_age_ms);
	}
	if (min_snapshots_to_keep.has_value()) {
		res.min_snapshots_to_keep.emplace();
		(*res.min_snapshots_to_keep) = (*min_snapshots_to_keep);
	}
	return res;
}

string SnapshotReference::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "SnapshotReference required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("SnapshotReference property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "SnapshotReference required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto max_ref_age_ms_val = yyjson_obj_get(obj, "max-ref-age-ms");
	if (max_ref_age_ms_val) {
		int64_t max_ref_age_ms_tmp;
		if (yyjson_is_sint(max_ref_age_ms_val)) {
			max_ref_age_ms_tmp = yyjson_get_sint(max_ref_age_ms_val);
		} else if (yyjson_is_uint(max_ref_age_ms_val)) {
			max_ref_age_ms_tmp = yyjson_get_uint(max_ref_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_ref_age_ms_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(max_ref_age_ms_val));
		}
		max_ref_age_ms = std::move(max_ref_age_ms_tmp);
	}
	auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max-snapshot-age-ms");
	if (max_snapshot_age_ms_val) {
		int64_t max_snapshot_age_ms_tmp;
		if (yyjson_is_sint(max_snapshot_age_ms_val)) {
			max_snapshot_age_ms_tmp = yyjson_get_sint(max_snapshot_age_ms_val);
		} else if (yyjson_is_uint(max_snapshot_age_ms_val)) {
			max_snapshot_age_ms_tmp = yyjson_get_uint(max_snapshot_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_snapshot_age_ms_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(max_snapshot_age_ms_val));
		}
		max_snapshot_age_ms = std::move(max_snapshot_age_ms_tmp);
	}
	auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min-snapshots-to-keep");
	if (min_snapshots_to_keep_val) {
		int32_t min_snapshots_to_keep_tmp;
		if (yyjson_is_int(min_snapshots_to_keep_val)) {
			min_snapshots_to_keep_tmp = yyjson_get_int(min_snapshots_to_keep_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'min_snapshots_to_keep_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(min_snapshots_to_keep_val));
		}
		min_snapshots_to_keep = std::move(min_snapshots_to_keep_tmp);
	}
	return "";
}

void SnapshotReference::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: max-ref-age-ms
	if (max_ref_age_ms.has_value()) {
		auto &max_ref_age_ms_value = *max_ref_age_ms;
		yyjson_mut_obj_add_sint(doc, obj, "max-ref-age-ms", max_ref_age_ms_value);
	}

	// Serialize: max-snapshot-age-ms
	if (max_snapshot_age_ms.has_value()) {
		auto &max_snapshot_age_ms_value = *max_snapshot_age_ms;
		yyjson_mut_obj_add_sint(doc, obj, "max-snapshot-age-ms", max_snapshot_age_ms_value);
	}

	// Serialize: min-snapshots-to-keep
	if (min_snapshots_to_keep.has_value()) {
		auto &min_snapshots_to_keep_value = *min_snapshots_to_keep;
		yyjson_mut_obj_add_int(doc, obj, "min-snapshots-to-keep", min_snapshots_to_keep_value);
	}
}

yyjson_mut_val *SnapshotReference::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
