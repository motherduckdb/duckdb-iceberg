
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "iceberg_metadata.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/load_table_result.hpp"

namespace duckdb {

class IRCatalog;

struct IRCAPISchema {
	//! The (potentially multiple) levels that the namespace is made up of
	vector<string> items;
	string catalog_name;
};

class IRCAPI {
public:
	static const string API_VERSION_1;
	static vector<string> GetCatalogs(ClientContext &context, IRCatalog &catalog);
	static vector<rest_api_objects::TableIdentifier> GetTables(ClientContext &context, IRCatalog &catalog,
	                                                           const string &schema);
	static rest_api_objects::LoadTableResult GetTable(ClientContext &context, IRCatalog &catalog, const string &schema,
	                                                  const string &table_name);
	static vector<IRCAPISchema> GetSchemas(ClientContext &context, IRCatalog &catalog, const vector<string> &parent);
};

} // namespace duckdb
