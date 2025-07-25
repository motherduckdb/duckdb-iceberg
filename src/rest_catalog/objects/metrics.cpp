
#include "rest_catalog/objects/metrics.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Metrics::Metrics() {
}

Metrics Metrics::FromJSON(yyjson_val *obj) {
	Metrics res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Metrics::TryFromJSON(yyjson_val *obj) {
	string error;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_str = yyjson_get_str(key);
		MetricResult tmp;
		error = tmp.TryFromJSON(val);
		if (!error.empty()) {
			return error;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
