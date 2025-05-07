
#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "url_utils.hpp"
#include "storage/irc_schema_set.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "storage/irc_authorization.hpp"

namespace duckdb {

class IRCSchemaEntry;

class ICRClearCacheFunction : public TableFunction {
public:
	ICRClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class MetadataCacheValue {
public:
	std::string data;
	std::chrono::system_clock::time_point expires_at;

public:
	MetadataCacheValue(std::string data_, std::chrono::system_clock::time_point expires_at_)
	    : data(data_), expires_at(expires_at_) {};
};

class IRCatalog : public Catalog {
public:
	explicit IRCatalog(AttachedDatabase &db_p, AccessMode access_mode, unique_ptr<IRCAuthorization> auth_handler,
	                   const string &warehouse, const string &uri, const string &version = "v1");
	~IRCatalog();

	string internal_name;
	AccessMode access_mode;
	unique_ptr<IRCAuthorization> auth_handler;
	IRCEndpointBuilder endpoint_builder;

	//! warehouse
	string warehouse;

	//! host of the REST catalog
	string uri;
	//! version
	const string version;
	//! optional prefix
	string prefix;

public:
	void Initialize(bool load_builtin) override;

	string GetCatalogType() override {
		return "iceberg";
	}

	optional_idx GetCatalogVersion(ClientContext &context) override {
		// Get it from the version string, I guess??
		return 1;
	}

	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetIcebergSecret(ClientContext &context, const string &secret_name);

	void GetConfig(ClientContext &context);

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	IRCEndpointBuilder GetBaseUrl() const;

	//! Whether or not this is an in-memory Iceberg database
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCache();

	bool HasCachedValue(string url) const;
	string GetCachedValue(string url) const;
	bool SetCachedValue(string url, const string &value, const rest_api_objects::LoadTableResult &result);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	IRCSchemaSet schemas;
	string default_schema;

	// defaults and overrides provided by a catalog.
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;

	unordered_map<string, unique_ptr<MetadataCacheValue>> metadata_cache;
};

} // namespace duckdb
