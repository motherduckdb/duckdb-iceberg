
#include "rest_catalog/objects/failed_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Object7 Object7::FromJSON(yyjson_val *obj) {
	Object7 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Object7::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "Object7 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

yyjson_mut_val *Object7::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: status
	yyjson_mut_val *status_val = status.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "status", status_val);

	return obj;
}

FailedPlanningResult FailedPlanningResult::FromJSON(yyjson_val *obj) {
	FailedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FailedPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = iceberg_error_response.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_7.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

yyjson_mut_val *FailedPlanningResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize base class: IcebergErrorResponse
	yyjson_mut_val *iceberg_error_responsebase_obj = iceberg_error_response.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(iceberg_error_responsebase_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	// Serialize base class: Object7
	yyjson_mut_val *object_7base_obj = object_7.ToJSON(doc);
	// Merge base properties into this object
	{
		size_t idx, max;
		yyjson_mut_val *key, *val;
		yyjson_mut_obj_foreach(object_7base_obj, idx, max, key, val) {
			yyjson_mut_obj_add(obj, key, val);
		}
	}

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
