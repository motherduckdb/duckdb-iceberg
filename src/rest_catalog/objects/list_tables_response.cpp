
#include "rest_catalog/objects/list_tables_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListTablesResponse::ListTablesResponse() {
}

ListTablesResponse ListTablesResponse::FromJSON(yyjson_val *obj) {
	ListTablesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ListTablesResponse ListTablesResponse::Copy() const {
	ListTablesResponse res;
	if (has_next_page_token) {
		res.next_page_token = next_page_token.Copy();
	}
	res.has_next_page_token = has_next_page_token;
	if (has_identifiers) {
		res.identifiers.reserve(identifiers.size());
		for (auto &item : identifiers) {
			res.identifiers.emplace_back(item.Copy());
		}
	}
	res.has_identifiers = has_identifiers;
	return res;
}

string ListTablesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
	if (next_page_token_val && !yyjson_is_null(next_page_token_val)) {
		has_next_page_token = true;
		error = next_page_token.TryFromJSON(next_page_token_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto identifiers_val = yyjson_obj_get(obj, "identifiers");
	if (identifiers_val && !yyjson_is_null(identifiers_val)) {
		has_identifiers = true;
		if (yyjson_is_arr(identifiers_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifiers_val, idx, max, val) {
				TableIdentifier tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				identifiers.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ListTablesResponse property 'identifiers' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(identifiers_val));
		}
	}
	return "";
}

yyjson_mut_val *ListTablesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: next-page-token
	if (has_next_page_token) {
		yyjson_mut_val *next_page_token_val = next_page_token.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "next-page-token", next_page_token_val);
	}

	// Serialize: identifiers
	if (has_identifiers) {
		yyjson_mut_val *identifiers_arr = yyjson_mut_arr(doc);
		for (const auto &item : identifiers) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(identifiers_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "identifiers", identifiers_arr);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
