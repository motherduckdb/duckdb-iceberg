
#include "rest_catalog/objects/data_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

DataFile DataFile::FromJSON(yyjson_val *obj) {
	DataFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string DataFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "DataFile required property 'content' is missing";
	} else {
		if (yyjson_is_str(content_val)) {
			content = yyjson_get_str(content_val);
		} else {
			return StringUtil::Format("DataFile property 'content' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(content_val));
		}
	}
	auto first_row_id_val = yyjson_obj_get(obj, "first-row-id");
	if (first_row_id_val) {
		has_first_row_id = true;
		if (yyjson_is_sint(first_row_id_val)) {
			first_row_id = yyjson_get_sint(first_row_id_val);
		} else if (yyjson_is_uint(first_row_id_val)) {
			first_row_id = yyjson_get_uint(first_row_id_val);
		} else {
			return StringUtil::Format("DataFile property 'first_row_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(first_row_id_val));
		}
	}
	auto column_sizes_val = yyjson_obj_get(obj, "column-sizes");
	if (column_sizes_val) {
		has_column_sizes = true;
		error = column_sizes.TryFromJSON(column_sizes_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_counts_val = yyjson_obj_get(obj, "value-counts");
	if (value_counts_val) {
		has_value_counts = true;
		error = value_counts.TryFromJSON(value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto null_value_counts_val = yyjson_obj_get(obj, "null-value-counts");
	if (null_value_counts_val) {
		has_null_value_counts = true;
		error = null_value_counts.TryFromJSON(null_value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto nan_value_counts_val = yyjson_obj_get(obj, "nan-value-counts");
	if (nan_value_counts_val) {
		has_nan_value_counts = true;
		error = nan_value_counts.TryFromJSON(nan_value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto lower_bounds_val = yyjson_obj_get(obj, "lower-bounds");
	if (lower_bounds_val) {
		has_lower_bounds = true;
		error = lower_bounds.TryFromJSON(lower_bounds_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto upper_bounds_val = yyjson_obj_get(obj, "upper-bounds");
	if (upper_bounds_val) {
		has_upper_bounds = true;
		error = upper_bounds.TryFromJSON(upper_bounds_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

yyjson_mut_val *DataFile::ToJSON(yyjson_mut_doc *doc) const {
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

	// Serialize: first-row-id
	if (has_first_row_id) {
		yyjson_mut_obj_add_sint(doc, obj, "first-row-id", first_row_id);
	}

	// Serialize: column-sizes
	if (has_column_sizes) {
		yyjson_mut_val *column_sizes_val = column_sizes.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "column-sizes", column_sizes_val);
	}

	// Serialize: value-counts
	if (has_value_counts) {
		yyjson_mut_val *value_counts_val = value_counts.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "value-counts", value_counts_val);
	}

	// Serialize: null-value-counts
	if (has_null_value_counts) {
		yyjson_mut_val *null_value_counts_val = null_value_counts.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "null-value-counts", null_value_counts_val);
	}

	// Serialize: nan-value-counts
	if (has_nan_value_counts) {
		yyjson_mut_val *nan_value_counts_val = nan_value_counts.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "nan-value-counts", nan_value_counts_val);
	}

	// Serialize: lower-bounds
	if (has_lower_bounds) {
		yyjson_mut_val *lower_bounds_val = lower_bounds.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "lower-bounds", lower_bounds_val);
	}

	// Serialize: upper-bounds
	if (has_upper_bounds) {
		yyjson_mut_val *upper_bounds_val = upper_bounds.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "upper-bounds", upper_bounds_val);
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
