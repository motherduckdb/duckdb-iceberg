
#include "rest_catalog/objects/equality_delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EqualityDeleteFile::EqualityDeleteFile() {
}

EqualityDeleteFile EqualityDeleteFile::FromJSON(yyjson_val *obj) {
	EqualityDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

EqualityDeleteFile EqualityDeleteFile::Copy() const {
	EqualityDeleteFile res;
	res.content_file = content_file.Copy();
	if (has_equality_ids) {
		res.equality_ids.reserve(equality_ids.size());
		for (auto &item : equality_ids) {
			res.equality_ids.emplace_back(item);
		}
	}
	res.has_equality_ids = has_equality_ids;
	return res;
}

string EqualityDeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto equality_ids_val = yyjson_obj_get(obj, "equality-ids");
	if (equality_ids_val && !yyjson_is_null(equality_ids_val)) {
		has_equality_ids = true;
		if (yyjson_is_arr(equality_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(equality_ids_val, idx, max, val) {
				int32_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format(
					    "EqualityDeleteFile property 'tmp' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				equality_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "EqualityDeleteFile property 'equality_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(equality_ids_val));
		}
	}
	return "";
}

void EqualityDeleteFile::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: ContentFile
	content_file.PopulateJSON(doc, obj);

	// Serialize: equality-ids
	if (has_equality_ids) {
		yyjson_mut_val *equality_ids_arr = yyjson_mut_arr(doc);
		for (const auto &item : equality_ids) {
			yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
			yyjson_mut_arr_append(equality_ids_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "equality-ids", equality_ids_arr);
	}
}

yyjson_mut_val *EqualityDeleteFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
