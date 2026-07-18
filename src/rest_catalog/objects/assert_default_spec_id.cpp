
#include "rest_catalog/objects/assert_default_spec_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSpecId::AssertDefaultSpecId() {
}

AssertDefaultSpecId AssertDefaultSpecId::FromJSON(yyjson_val *obj) {
	AssertDefaultSpecId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertDefaultSpecId AssertDefaultSpecId::Copy() const {
	AssertDefaultSpecId res;
	res.table_requirement = table_requirement.Copy();
	res.default_spec_id = default_spec_id;
	return res;
}

string AssertDefaultSpecId::TryFromJSON(yyjson_val *obj) {
	string error;
	error = table_requirement.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
	if (!default_spec_id_val) {
		return "AssertDefaultSpecId required property 'default-spec-id' is missing";
	} else {
		if (yyjson_is_int(default_spec_id_val)) {
			default_spec_id = yyjson_get_int(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "AssertDefaultSpecId property 'default_spec_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_spec_id_val));
		}
	}
	return "";
}

void AssertDefaultSpecId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: TableRequirement
	table_requirement.PopulateJSON(doc, obj);

	// Serialize: default-spec-id
	yyjson_mut_obj_add_int(doc, obj, "default-spec-id", default_spec_id);
}

yyjson_mut_val *AssertDefaultSpecId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
