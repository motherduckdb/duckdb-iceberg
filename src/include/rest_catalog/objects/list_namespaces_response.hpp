
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/page_token.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListNamespacesResponse {
public:
	ListNamespacesResponse();
	ListNamespacesResponse(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse &operator=(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse(ListNamespacesResponse &&) = default;
	ListNamespacesResponse &operator=(ListNamespacesResponse &&) = default;

public:
	// Deserialization
	static ListNamespacesResponse FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	ListNamespacesResponse Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	PageToken next_page_token;
	bool has_next_page_token = false;
	vector<Namespace> namespaces;
	bool has_namespaces = false;
};

} // namespace rest_api_objects
} // namespace duckdb
