
#include "rest_catalog/objects/add_sort_order_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AddSortOrderUpdate::AddSortOrderUpdate() {
}

AddSortOrderUpdate AddSortOrderUpdate::FromJSON(yyjson_val *obj) {
	AddSortOrderUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddSortOrderUpdate AddSortOrderUpdate::Copy() const {
	AddSortOrderUpdate res;
	res.base_update = base_update.Copy();
	res.sort_order = sort_order.Copy();
	return res;
}

string AddSortOrderUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto sort_order_val = yyjson_obj_get(obj, "sort-order");
	if (!sort_order_val) {
		return "AddSortOrderUpdate required property 'sort-order' is missing";
	} else {
		error = sort_order.TryFromJSON(sort_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddSortOrderUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: sort-order
	yyjson_mut_val *sort_order_val = sort_order.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "sort-order", sort_order_val);
}

yyjson_mut_val *AddSortOrderUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
