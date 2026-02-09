
#include "rest_catalog/objects/struct_field.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StructField StructField::FromJSON(yyjson_val *obj) {
	StructField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string StructField::TryFromJSON(yyjson_val *obj) {
	string error;
	auto id_val = yyjson_obj_get(obj, "id");
	if (!id_val) {
		return "StructField required property 'id' is missing";
	} else {
		if (yyjson_is_int(id_val)) {
			id = yyjson_get_int(id_val);
		} else {
			return StringUtil::Format("StructField property 'id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(id_val));
		}
	}
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "StructField required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("StructField property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "StructField required property 'type' is missing";
	} else {
		type = make_uniq<Type>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto required_val = yyjson_obj_get(obj, "required");
	if (!required_val) {
		return "StructField required property 'required' is missing";
	} else {
		if (yyjson_is_bool(required_val)) {
			required = yyjson_get_bool(required_val);
		} else {
			return StringUtil::Format("StructField property 'required' is not of type 'boolean', found '%s' instead",
			                          yyjson_get_type_desc(required_val));
		}
	}
	auto _doc_val = yyjson_obj_get(obj, "doc");
	if (_doc_val) {
		has__doc = true;
		if (yyjson_is_str(_doc_val)) {
			_doc = yyjson_get_str(_doc_val);
		} else {
			return StringUtil::Format("StructField property '_doc' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(_doc_val));
		}
	}
	auto initial_default_val = yyjson_obj_get(obj, "initial-default");
	if (initial_default_val) {
		has_initial_default = true;
		error = initial_default.TryFromJSON(initial_default_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto write_default_val = yyjson_obj_get(obj, "write-default");
	if (write_default_val) {
		has_write_default = true;
		error = write_default.TryFromJSON(write_default_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

yyjson_mut_val *StructField::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: id
	yyjson_mut_obj_add_int(doc, obj, "id", id);

	// Serialize: name
	yyjson_mut_obj_add_str(doc, obj, "name", name.c_str());

	// Serialize: type
	yyjson_mut_val *type_val = type->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "type", type_val);

	// Serialize: required
	yyjson_mut_obj_add_bool(doc, obj, "required", required);

	// Serialize: doc
	if (has__doc) {
		yyjson_mut_obj_add_str(doc, obj, "doc", _doc.c_str());
	}

	// Serialize: initial-default
	if (has_initial_default) {
		yyjson_mut_val *initial_default_val = initial_default.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "initial-default", initial_default_val);
	}

	// Serialize: write-default
	if (has_write_default) {
		yyjson_mut_val *write_default_val = write_default.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "write-default", write_default_val);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
