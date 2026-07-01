
#include "rest_catalog/objects/set_current_schema_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetCurrentSchemaUpdate::SetCurrentSchemaUpdate() {
}

SetCurrentSchemaUpdate SetCurrentSchemaUpdate::FromJSON(yyjson_val *obj) {
	SetCurrentSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetCurrentSchemaUpdate SetCurrentSchemaUpdate::Copy() const {
	SetCurrentSchemaUpdate res;
	res.base_update = base_update.Copy();
	res.schema_id = schema_id;
	return res;
}

string SetCurrentSchemaUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (!schema_id_val) {
		return "SetCurrentSchemaUpdate required property 'schema-id' is missing";
	} else {
		if (yyjson_is_int(schema_id_val)) {
			schema_id = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentSchemaUpdate property 'schema_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(schema_id_val));
		}
	}
	return "";
}

void SetCurrentSchemaUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: schema-id
	yyjson_mut_obj_add_int(doc, obj, "schema-id", schema_id);
}

yyjson_mut_val *SetCurrentSchemaUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
