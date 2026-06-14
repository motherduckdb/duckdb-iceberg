
#include "rest_catalog/objects/counter_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CounterResult::CounterResult() {
}

CounterResult CounterResult::FromJSON(yyjson_val *obj) {
	CounterResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CounterResult CounterResult::Copy() const {
	CounterResult res;
	res.unit = unit;
	res.value = value;
	return res;
}

string CounterResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto unit_val = yyjson_obj_get(obj, "unit");
	if (!unit_val) {
		return "CounterResult required property 'unit' is missing";
	} else {
		if (yyjson_is_str(unit_val)) {
			unit = yyjson_get_str(unit_val);
		} else {
			return StringUtil::Format("CounterResult property 'unit' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(unit_val));
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "CounterResult required property 'value' is missing";
	} else {
		if (yyjson_is_sint(value_val)) {
			value = yyjson_get_sint(value_val);
		} else if (yyjson_is_uint(value_val)) {
			value = yyjson_get_uint(value_val);
		} else {
			return StringUtil::Format("CounterResult property 'value' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(value_val));
		}
	}
	return "";
}

void CounterResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: unit
	yyjson_mut_obj_add_strcpy(doc, obj, "unit", unit.c_str());

	// Serialize: value
	yyjson_mut_obj_add_sint(doc, obj, "value", value);
}

yyjson_mut_val *CounterResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
