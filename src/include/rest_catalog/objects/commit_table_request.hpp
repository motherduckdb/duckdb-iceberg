
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/table_requirement.hpp"
#include "rest_catalog/objects/table_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableRequest {
public:
	CommitTableRequest();
	CommitTableRequest(const CommitTableRequest &) = delete;
	CommitTableRequest &operator=(const CommitTableRequest &) = delete;
	CommitTableRequest(CommitTableRequest &&) = default;
	CommitTableRequest &operator=(CommitTableRequest &&) = default;

public:
	// Deserialization
	static CommitTableRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CommitTableRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<TableRequirement> requirements;
	vector<TableUpdate> updates;
	TableIdentifier identifier;
	bool has_identifier = false;
};

} // namespace rest_api_objects
} // namespace duckdb
