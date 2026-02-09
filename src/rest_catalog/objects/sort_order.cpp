
#include "rest_catalog/objects/sort_order.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SortOrder SortOrder::FromJSON(yyjson_val *obj) {
	SortOrder res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SortOrder::TryFromJSON(yyjson_val *obj) {
	string error;
	auto order_id_val = yyjson_obj_get(obj, "order-id");
	if (!order_id_val) {
		return "SortOrder required property 'order-id' is missing";
	} else {
		if (yyjson_is_int(order_id_val)) {
			order_id = yyjson_get_int(order_id_val);
		} else {
			return StringUtil::Format("SortOrder property 'order_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(order_id_val));
		}
	}
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "SortOrder required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				SortField tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("SortOrder property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	return "";
}

yyjson_mut_val *SortOrder::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: order-id
	yyjson_mut_obj_add_int(doc, obj, "order-id", order_id);

	// Serialize: fields
	yyjson_mut_val *fields_arr = yyjson_mut_arr(doc);
	for (const auto &item : fields) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(fields_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "fields", fields_arr);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
