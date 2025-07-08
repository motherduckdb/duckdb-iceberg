
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "iceberg_metadata.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/load_table_result.hpp"

namespace duckdb {

class IRCatalog;
class IRCSchemaEntry;

class IRCAPI {
public:
	static const string API_VERSION_1;
	static vector<string> GetCatalogs(ClientContext &context, IRCatalog &catalog);
	static void GetTables(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema,
	                      vector<rest_api_objects::TableIdentifier> &out);
	static rest_api_objects::LoadTableResult GetTable(ClientContext &context, IRCatalog &catalog,
	                                                  IRCSchemaEntry &schema, const string &table_name);
	static vector<string> GetSchemas(ClientContext &context, IRCatalog &catalog);
	static bool VerifySchemaExistence(ClientContext &context, IRCatalog &catalog, const string &schema);
	static bool VerifyTableExistence(ClientContext &context, IRCatalog &catalog, const IRCSchemaEntry &schema,
	                                 const string &table);
};

} // namespace duckdb
