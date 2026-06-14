
#include "rest_catalog/objects/remove_partition_specs_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePartitionSpecsUpdate::RemovePartitionSpecsUpdate() {
}

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::FromJSON(yyjson_val *obj) {
	RemovePartitionSpecsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::Copy() const {
	RemovePartitionSpecsUpdate res;
	res.base_update = base_update.Copy();
	res.spec_ids.reserve(spec_ids.size());
	for (auto &item : spec_ids) {
		res.spec_ids.emplace_back(item);
	}
	return res;
}

string RemovePartitionSpecsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto spec_ids_val = yyjson_obj_get(obj, "spec-ids");
	if (!spec_ids_val) {
		return "RemovePartitionSpecsUpdate required property 'spec-ids' is missing";
	} else {
		if (yyjson_is_arr(spec_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(spec_ids_val, idx, max, val) {
				int32_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format(
					    "RemovePartitionSpecsUpdate property 'tmp' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				spec_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "RemovePartitionSpecsUpdate property 'spec_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(spec_ids_val));
		}
	}
	return "";
}

void RemovePartitionSpecsUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: spec-ids
	yyjson_mut_val *spec_ids_arr = yyjson_mut_arr(doc);
	for (const auto &item : spec_ids) {
		yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
		yyjson_mut_arr_append(spec_ids_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "spec-ids", spec_ids_arr);
}

yyjson_mut_val *RemovePartitionSpecsUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
