
#include "rest_catalog/objects/list_namespaces_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListNamespacesResponse::ListNamespacesResponse() {
}

ListNamespacesResponse ListNamespacesResponse::FromJSON(yyjson_val *obj) {
	ListNamespacesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ListNamespacesResponse ListNamespacesResponse::Copy() const {
	ListNamespacesResponse res;
	if (has_next_page_token) {
		res.next_page_token = next_page_token.Copy();
	}
	res.has_next_page_token = has_next_page_token;
	if (has_namespaces) {
		res.namespaces.reserve(namespaces.size());
		for (auto &item : namespaces) {
			res.namespaces.emplace_back(item.Copy());
		}
	}
	res.has_namespaces = has_namespaces;
	return res;
}

string ListNamespacesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
	if (next_page_token_val && !yyjson_is_null(next_page_token_val)) {
		has_next_page_token = true;
		error = next_page_token.TryFromJSON(next_page_token_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto namespaces_val = yyjson_obj_get(obj, "namespaces");
	if (namespaces_val && !yyjson_is_null(namespaces_val)) {
		has_namespaces = true;
		if (yyjson_is_arr(namespaces_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(namespaces_val, idx, max, val) {
				Namespace tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				namespaces.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ListNamespacesResponse property 'namespaces' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(namespaces_val));
		}
	}
	return "";
}

void ListNamespacesResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: next-page-token
	if (has_next_page_token) {
		yyjson_mut_val *next_page_token_val = next_page_token.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "next-page-token", next_page_token_val);
	}

	// Serialize: namespaces
	if (has_namespaces) {
		yyjson_mut_val *namespaces_arr = yyjson_mut_arr(doc);
		for (const auto &item : namespaces) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(namespaces_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "namespaces", namespaces_arr);
	}
}

yyjson_mut_val *ListNamespacesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
