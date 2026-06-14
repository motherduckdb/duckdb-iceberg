
#include "rest_catalog/objects/add_schema_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddSchemaUpdate::AddSchemaUpdate() {
}

AddSchemaUpdate AddSchemaUpdate::FromJSON(yyjson_val *obj) {
	AddSchemaUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSchemaUpdate AddSchemaUpdate::Copy() const {
	AddSchemaUpdate res;
	res.base_update = base_update.Copy();
	res.schema = schema.Copy();
	if (has_last_column_id) {
		res.last_column_id = last_column_id;
	}
	res.has_last_column_id = has_last_column_id;
	return res;
}

string AddSchemaUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto schema_val = yyjson_obj_get(obj, "schema");
	if (!schema_val) {
		return "AddSchemaUpdate required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
	if (last_column_id_val && !yyjson_is_null(last_column_id_val)) {
		has_last_column_id = true;
		if (yyjson_is_int(last_column_id_val)) {
			last_column_id = yyjson_get_int(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "AddSchemaUpdate property 'last_column_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_column_id_val));
		}
	}
	return "";
}

void AddSchemaUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: schema
	yyjson_mut_val *schema_val = schema.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "schema", schema_val);

	// Serialize: last-column-id
	if (has_last_column_id) {
		yyjson_mut_obj_add_int(doc, obj, "last-column-id", last_column_id);
	}
}

yyjson_mut_val *AddSchemaUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
