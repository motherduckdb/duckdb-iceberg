
#include "rest_catalog/objects/sort_field.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SortField::SortField() {
}

SortField SortField::FromJSON(yyjson_val *obj) {
	SortField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SortField SortField::Copy() const {
	SortField res;
	res.source_id = source_id;
	res.transform = transform.Copy();
	res.direction = direction.Copy();
	res.null_order = null_order.Copy();
	return res;
}

string SortField::TryFromJSON(yyjson_val *obj) {
	string error;
	auto source_id_val = yyjson_obj_get(obj, "source-id");
	if (!source_id_val) {
		return "SortField required property 'source-id' is missing";
	} else {
		if (yyjson_is_int(source_id_val)) {
			source_id = yyjson_get_int(source_id_val);
		} else {
			return StringUtil::Format("SortField property 'source_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(source_id_val));
		}
	}
	auto transform_val = yyjson_obj_get(obj, "transform");
	if (!transform_val) {
		return "SortField required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto direction_val = yyjson_obj_get(obj, "direction");
	if (!direction_val) {
		return "SortField required property 'direction' is missing";
	} else {
		error = direction.TryFromJSON(direction_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto null_order_val = yyjson_obj_get(obj, "null-order");
	if (!null_order_val) {
		return "SortField required property 'null-order' is missing";
	} else {
		error = null_order.TryFromJSON(null_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void SortField::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: source-id
	yyjson_mut_obj_add_int(doc, obj, "source-id", source_id);

	// Serialize: transform
	yyjson_mut_val *transform_val = transform.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "transform", transform_val);

	// Serialize: direction
	yyjson_mut_val *direction_val = direction.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "direction", direction_val);

	// Serialize: null-order
	yyjson_mut_val *null_order_val = null_order.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "null-order", null_order_val);
}

yyjson_mut_val *SortField::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
