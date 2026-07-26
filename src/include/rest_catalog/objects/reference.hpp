
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Reference {
public:
	Reference();
	Reference(const Reference &) = delete;
	Reference &operator=(const Reference &) = delete;
	Reference(Reference &&) = default;
	Reference &operator=(Reference &&) = default;

public:
	// Deserialization
	static Reference FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Reference Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
