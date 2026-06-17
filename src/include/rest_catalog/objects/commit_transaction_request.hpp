
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTransactionRequest {
public:
	CommitTransactionRequest();
	CommitTransactionRequest(const CommitTransactionRequest &) = delete;
	CommitTransactionRequest &operator=(const CommitTransactionRequest &) = delete;
	CommitTransactionRequest(CommitTransactionRequest &&) = default;
	CommitTransactionRequest &operator=(CommitTransactionRequest &&) = default;

public:
	// Deserialization
	static CommitTransactionRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CommitTransactionRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<CommitTableRequest> table_changes;
};

} // namespace rest_api_objects
} // namespace duckdb
