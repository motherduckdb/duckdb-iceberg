
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PageToken {
public:
	PageToken();
	PageToken(const PageToken &) = delete;
	PageToken &operator=(const PageToken &) = delete;
	PageToken(PageToken &&) = default;
	PageToken &operator=(PageToken &&) = default;

public:
	// Deserialization
	static PageToken FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	PageToken Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
