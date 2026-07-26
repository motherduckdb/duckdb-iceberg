
#include "rest_catalog/objects/metrics.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Metrics::Metrics() {
}

Metrics Metrics::FromJSON(JSONValue obj) {
	Metrics res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Metrics Metrics::Copy() const {
	Metrics res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second.Copy());
	}
	return res;
}

string Metrics::TryFromJSON(JSONValue obj) {
	string error;
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		MetricResult tmp;
		error = tmp.TryFromJSON(val);
		if (!error.empty()) {
			return;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	});
	if (!error.empty()) {
		return error;
	}
	return "";
}

void Metrics::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize additional properties
	for (const auto &it : additional_properties) {
		auto &key = it.first;
		auto &value = it.second;
		auto value_obj = value.ToJSON(writer);
		obj.Add(key, value_obj);
	}
}

JSONMutableValue Metrics::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
