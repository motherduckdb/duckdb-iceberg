#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/irc_catalog.hpp"
#include "storage/irc_table_set.hpp"

#include "storage/irc_transaction.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/irc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "storage/irc_transaction.hpp"

#include "storage/authorization/sigv4.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

// IcebergTableInformation::IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name)
//    : catalog(catalog), schema(schema), name(name) {
//	table_id = "uuid-" + schema.name + "-" + name;
//}

// void IcebergTableInformation::AddSnapshot(IRCTransaction &transaction, vector<IcebergManifestEntry> &&data_files) {
//	if (!transaction_data) {
//		auto context = transaction.context.lock();
//		transaction_data = make_uniq<IcebergTransactionData>(*context, *this);
//	}

//	transaction_data->AddSnapshot(IcebergSnapshotOperationType::APPEND, std::move(data_files));
//}

// static void ParseConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options)
// {
//	//! Set of recognized config parameters and the duckdb secret option that matches it.
//	static const case_insensitive_map_t<string> config_to_option = {{"s3.access-key-id", "key_id"},
//	                                                                {"s3.secret-access-key", "secret"},
//	                                                                {"s3.session-token", "session_token"},
//	                                                                {"s3.region", "region"},
//	                                                                {"region", "region"},
//	                                                                {"client.region", "region"},
//	                                                                {"s3.endpoint", "endpoint"}};

//	if (config.empty()) {
//		return;
//	}
//	for (auto &entry : config) {
//		auto it = config_to_option.find(entry.first);
//		if (it != config_to_option.end()) {
//			options[it->second] = entry.second;
//		}
//	}

//	auto it = config.find("s3.path-style-access");
//	if (it != config.end()) {
//		bool path_style;
//		if (it->second == "true") {
//			path_style = true;
//		} else if (it->second == "false") {
//			path_style = false;
//		} else {
//			throw InvalidInputException("Unexpected value ('%s') for 's3.path-style-access' in 'config' property",
//			                            it->second);
//		}

//		options["use_ssl"] = Value(!path_style);
//		if (path_style) {
//			options["url_style"] = "path";
//		}
//	}

//	auto endpoint_it = options.find("endpoint");
//	if (endpoint_it == options.end()) {
//		return;
//	}
//	auto endpoint = endpoint_it->second.ToString();
//	if (StringUtil::StartsWith(endpoint, "http://")) {
//		endpoint = endpoint.substr(7, string::npos);
//	}
//	if (StringUtil::StartsWith(endpoint, "https://")) {
//		endpoint = endpoint.substr(8, string::npos);
//	}
//	if (StringUtil::EndsWith(endpoint, "/")) {
//		endpoint = endpoint.substr(0, endpoint.size() - 1);
//	}
//	endpoint_it->second = endpoint;
//}

// const string &IcebergTableInformation::BaseFilePath() const {
//	return load_table_result.metadata.location;
//}

// optional_ptr<CatalogEntry> IcebergTableInformation::CreateSchemaVersion(IcebergTableSchema &table_schema) {
//	CreateTableInfo info;
//	info.table = name;
//	for (auto &col : table_schema.columns) {
//		info.columns.AddColumn(ColumnDefinition(col->name, col->type));
//	}

//	auto table_entry = make_uniq<ICTableEntry>(*this, catalog, schema, info);
//	if (!table_entry->internal) {
//		table_entry->internal = schema.internal;
//	}
//	auto result = table_entry.get();
//	if (result->name.empty()) {
//		throw InternalException("ICTableSet::CreateEntry called with empty name");
//	}
//	schema_versions.emplace(table_schema.schema_id, std::move(table_entry));
//	return result;
//}

// optional_ptr<CatalogEntry> IcebergTableInformation::GetSchemaVersion(optional_ptr<BoundAtClause> at) {
//	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);

//	int32_t schema_id;
//	if (snapshot_lookup.IsLatest()) {
//		schema_id = table_metadata.current_schema_id;
//	} else {
//		auto snapshot = table_metadata.GetSnapshot(snapshot_lookup);
//		D_ASSERT(snapshot);
//		schema_id = snapshot->schema_id;
//	}
//	return schema_versions[schema_id].get();
//}

ICTableSet::ICTableSet(IRCSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

void ICTableSet::FillEntry(ClientContext &context, IcebergTableInformation &table) {
	if (!table.schema_versions.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	table.load_table_result = IRCAPI::GetTable(context, ic_catalog, schema, table.name);
	table.table_metadata = IcebergTableMetadata::FromTableMetadata(table.load_table_result.metadata);
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
}

void ICTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		auto &table_info = entry.second;
		FillEntry(context, table_info);
		auto schema_id = table_info.table_metadata.current_schema_id;
		callback(*table_info.schema_versions[schema_id]);
	}
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (listed) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	vector<rest_api_objects::TableIdentifier> tables;
	IRCAPI::GetTables(context, ic_catalog, schema, tables);

	for (auto &table : tables) {
		auto entry_it = entries.find(table.name);
		if (entry_it == entries.end()) {
			entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
		}
	}
	listed = true;
}

void ICTableSet::CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema,
                                CreateTableInfo &info) {
	auto table_name = info.table;
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw InvalidInputException("CREATE OR REPLACE not supported in DuckDB-Iceberg");
	}
	if (entries.find(table_name) != entries.end()) {
		throw CatalogException("Table %s already exists", table_name.c_str());
	}

	entries.emplace(table_name, IcebergTableInformation(catalog, schema, info.table));
	auto &table_info = entries.find(table_name)->second;
	auto &irc_transaction = IRCTransaction::Get(context, catalog);

	auto table_entry = make_uniq<ICTableEntry>(table_info, catalog, schema, info);
	auto optional_entry = table_entry.get();

	optional_entry->table_info.schema_versions[0] = std::move(table_entry);
	optional_entry->table_info.table_metadata.schemas[0] =
	    IcebergCreateTableRequest::CreateIcebergSchema(optional_entry);
	optional_entry->table_info.table_metadata.current_schema_id = 0;
	optional_entry->table_info.table_metadata.schemas[0]->schema_id = 0;
	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto load_table_result = IRCAPI::CommitNewTable(context, catalog, optional_entry);
	optional_entry->table_info.load_table_result = std::move(load_table_result);
	optional_entry->table_info.table_metadata =
	    IcebergTableMetadata::FromTableMetadata(optional_entry->table_info.load_table_result.metadata);

	if (catalog.attach_options.supports_stage_create) {
		// We have a response from the server for a stage create, we need to also send a number of table
		// updates to finalize creation of the table.
		table_info.AddAssertCreate(irc_transaction);
		table_info.AddAssignUUID(irc_transaction);
		table_info.AddUpradeFormatVersion(irc_transaction);
		table_info.AddSchema(irc_transaction);
		table_info.AddSetCurrentSchema(irc_transaction);
		table_info.AddPartitionSpec(irc_transaction);
		table_info.SetDefaultSpec(irc_transaction);
		table_info.AddSortOrder(irc_transaction);
		table_info.SetDefaultSortOrder(irc_transaction);
		table_info.SetLocation(irc_transaction);
	}
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto table_name = lookup.GetEntryName();
	auto entry = entries.find(table_name);
	if (entry == entries.end()) {
		if (!IRCAPI::VerifyTableExistence(context, ic_catalog, schema, table_name)) {
			return nullptr;
		}
		auto it = entries.emplace(table_name, IcebergTableInformation(ic_catalog, schema, table_name));
		entry = it.first;
	}
	if (entry->second.transaction_data && entry->second.transaction_data->is_deleted) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.GetSchemaVersion(lookup.GetAtClause());
}

} // namespace duckdb
