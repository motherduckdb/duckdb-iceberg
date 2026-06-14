
#include "rest_catalog/objects/add_partition_spec_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddPartitionSpecUpdate::AddPartitionSpecUpdate() {
}

AddPartitionSpecUpdate AddPartitionSpecUpdate::FromJSON(yyjson_val *obj) {
	AddPartitionSpecUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddPartitionSpecUpdate AddPartitionSpecUpdate::Copy() const {
	AddPartitionSpecUpdate res;
	res.base_update = base_update.Copy();
	res.spec = spec.Copy();
	return res;
}

string AddPartitionSpecUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto spec_val = yyjson_obj_get(obj, "spec");
	if (!spec_val) {
		return "AddPartitionSpecUpdate required property 'spec' is missing";
	} else {
		error = spec.TryFromJSON(spec_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddPartitionSpecUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: spec
	yyjson_mut_val *spec_val = spec.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "spec", spec_val);
}

yyjson_mut_val *AddPartitionSpecUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
