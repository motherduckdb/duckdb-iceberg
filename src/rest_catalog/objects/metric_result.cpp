
#include "rest_catalog/objects/metric_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

MetricResult MetricResult::FromJSON(yyjson_val *obj) {
	MetricResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string MetricResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = counter_result.TryFromJSON(obj);
	if (error.empty()) {
		has_counter_result = true;
	}
	error = timer_result.TryFromJSON(obj);
	if (error.empty()) {
		has_timer_result = true;
	}
	if (!has_counter_result && !has_timer_result) {
		return "MetricResult failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

yyjson_mut_val *MetricResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
