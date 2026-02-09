
#include "rest_catalog/objects/position_delete_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PositionDeleteFile PositionDeleteFile::FromJSON(yyjson_val *obj) {
	PositionDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PositionDeleteFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "PositionDeleteFile required property 'content' is missing";
	} else {
		if (yyjson_is_str(content_val)) {
			content = yyjson_get_str(content_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(content_val));
		}
	}
	auto content_offset_val = yyjson_obj_get(obj, "content-offset");
	if (content_offset_val) {
		has_content_offset = true;
		if (yyjson_is_sint(content_offset_val)) {
			content_offset = yyjson_get_sint(content_offset_val);
		} else if (yyjson_is_uint(content_offset_val)) {
			content_offset = yyjson_get_uint(content_offset_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_offset' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(content_offset_val));
		}
	}
	auto content_size_in_bytes_val = yyjson_obj_get(obj, "content-size-in-bytes");
	if (content_size_in_bytes_val) {
		has_content_size_in_bytes = true;
		if (yyjson_is_sint(content_size_in_bytes_val)) {
			content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
		} else if (yyjson_is_uint(content_size_in_bytes_val)) {
			content_size_in_bytes = yyjson_get_uint(content_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PositionDeleteFile property 'content_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(content_size_in_bytes_val));
		}
	}
	return "";
}

yyjson_mut_val *PositionDeleteFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: ContentFile
	yyjson_mut_val *content_filebase_obj = content_file.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(content_filebase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize: content
	yyjson_mut_obj_add_str(doc, obj, "content", content.c_str());

	// Serialize: content-offset
	if (has_content_offset) {
		yyjson_mut_obj_add_sint(doc, obj, "content-offset", content_offset);
	}

	// Serialize: content-size-in-bytes
	if (has_content_size_in_bytes) {
		yyjson_mut_obj_add_sint(doc, obj, "content-size-in-bytes", content_size_in_bytes);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
