
#include "rest_catalog/objects/table_identifier.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TableIdentifier TableIdentifier::FromJSON(yyjson_val *obj) {
	TableIdentifier res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TableIdentifier::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _namespace_val = yyjson_obj_get(obj, "namespace");
	if (!_namespace_val) {
		return "TableIdentifier required property 'namespace' is missing";
	} else {
		error = _namespace.TryFromJSON(_namespace_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "TableIdentifier required property 'name' is missing";
	} else {
		if (yyjson_is_null(name_val)) {
			return "TableIdentifier property 'name' is not nullable, but is 'null'";
		} else if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("TableIdentifier property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	return "";
}

yyjson_mut_val *TableIdentifier::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: namespace
	yyjson_mut_val *_namespace_val = _namespace.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "namespace", _namespace_val);

	// Serialize: name
	yyjson_mut_obj_add_str(doc, obj, "name", name.c_str());

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
