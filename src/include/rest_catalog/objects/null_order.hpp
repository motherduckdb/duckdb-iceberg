
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class NullOrder {
public:
	NullOrder();
	NullOrder(const NullOrder &) = delete;
	NullOrder &operator=(const NullOrder &) = delete;
	NullOrder(NullOrder &&) = default;
	NullOrder &operator=(NullOrder &&) = default;

public:
	// Deserialization
	static NullOrder FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	NullOrder Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
