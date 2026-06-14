
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Namespace {
public:
	Namespace();
	Namespace(const Namespace &) = delete;
	Namespace &operator=(const Namespace &) = delete;
	Namespace(Namespace &&) = default;
	Namespace &operator=(Namespace &&) = default;

public:
	// Deserialization
	static Namespace FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	Namespace Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<string> value;
};

} // namespace rest_api_objects
} // namespace duckdb
