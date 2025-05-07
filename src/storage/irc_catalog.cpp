#include "storage/irc_schema_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "iceberg_utils.hpp"
#include "api_utils.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "rest_catalog/objects/catalog_config.hpp"
#include "storage/irc_catalog.hpp"
#include "curl.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

IRCatalog::IRCatalog(AttachedDatabase &db_p, AccessMode access_mode, unique_ptr<IRCAuthorization> auth_handler,
                     const string &warehouse, const string &uri, const string &version)
    : Catalog(db_p), access_mode(access_mode), auth_handler(std::move(auth_handler)), warehouse(warehouse), uri(uri),
      version(version), schemas(*this) {
	if (version.empty()) {
		throw InternalException("version can not be empty");
	}
}

IRCatalog::~IRCatalog() = default;

void IRCatalog::Initialize(bool load_builtin) {
}

void IRCatalog::GetConfig(ClientContext &context) {
	auto url = GetBaseUrl();
	// set the prefix to be empty. To get the config endpoint,
	// we cannot add a default prefix.
	D_ASSERT(prefix.empty());
	url.AddPathComponent("config");
	url.SetParam("warehouse", warehouse);
	RequestInput request_input;
	auto response = auth_handler->GetRequest(context, url, request_input);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response));
	auto *root = yyjson_doc_get_root(doc.get());
	auto catalog_config = rest_api_objects::CatalogConfig::FromJSON(root);

	overrides = catalog_config.overrides;
	defaults = catalog_config.defaults;
	// save overrides and defaults.
	// See https://iceberg.apache.org/docs/latest/configuration/#catalog-properties for sometimes used catalog
	// properties

	auto default_prefix_it = defaults.find("prefix");
	auto override_prefix_it = overrides.find("prefix");

	if (default_prefix_it != defaults.end()) {
		// sometimes there is a prefix in the defaults
		prefix = StringUtil::URLDecode(default_prefix_it->second);
		defaults.erase(default_prefix_it);
	}
	if (override_prefix_it != overrides.end()) {
		// sometimes the prefix in the overrides. Prefer the override prefix
		prefix = StringUtil::URLDecode(override_prefix_it->second);
		overrides.erase(override_prefix_it);
	}

	if (prefix.empty()) {
		DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.HttpRequest", "No prefix found for catalog with warehouse value %s",
		                 warehouse);
	}
	// TODO: store optional endpoints param as well. We can enforce per catalog the endpoints that
	//  are allowed to be hit
}

optional_ptr<CatalogEntry> IRCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropSchema(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void IRCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropSchema(context, info);
}

void IRCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<IRCSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> IRCatalog::LookupSchema(CatalogTransaction transaction,
                                                         const EntryLookupInfo &schema_lookup,
                                                         OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			if (if_not_found == OnEntryNotFound::RETURN_NULL) {
				return nullptr;
			}
			throw CatalogException(
			    schema_lookup.GetErrorContext(),
			    "Attempting to fetch the default schema - but no database was provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name \"%s\" not found", schema_name);
	}

	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool IRCatalog::InMemory() {
	return false;
}

string IRCatalog::GetDBPath() {
	return internal_name;
}

DatabaseSize IRCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

IRCEndpointBuilder IRCatalog::GetBaseUrl() const {
	auto base_url = IRCEndpointBuilder();
	base_url.SetHost(uri);
	base_url.AddPathComponent(version);
	return base_url;
}

void IRCatalog::ClearCache() {
	schemas.ClearEntries();
}

unique_ptr<SecretEntry> IRCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	case_insensitive_set_t accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			if (accepted_secret_types.count(secret_type)) {
				return secret_entry;
			}
			throw InvalidConfigurationException(
			    "Found a secret by the name of '%s', but it is not of an accepted type for a 'secret', "
			    "accepted types are: 's3' or 'aws', found '%s'",
			    secret_name, secret_type);
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}

	for (auto &type : accepted_secret_types) {
		if (secret_name.empty()) {
			//! Lookup the default secret for this type
			auto secret_entry =
			    context.db->GetSecretManager().GetSecretByName(transaction, StringUtil::Format("__default_%s", type));
			if (secret_entry) {
				return secret_entry;
			}
		}
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, type + "://", type);
		if (secret_match.HasMatch()) {
			return std::move(secret_match.secret_entry);
		}
	}
	throw InvalidConfigurationException("Could not find a valid storage secret (s3 or aws)");
}

unique_ptr<SecretEntry> IRCatalog::GetIcebergSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> secret_entry = nullptr;
	if (secret_name.empty()) {
		//! Try to find any secret with type 'iceberg'
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "", "iceberg");
		if (!secret_match.HasMatch()) {
			return nullptr;
		}
		secret_entry = std::move(secret_match.secret_entry);
	} else {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
	}
	return secret_entry;
}

PhysicalOperator &IRCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                        optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanInsert");
}
PhysicalOperator &IRCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("ICCatalog PlanCreateTableAs");
}
PhysicalOperator &IRCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                        PhysicalOperator &plan) {
	throw NotImplementedException("ICCatalog PlanDelete");
}
PhysicalOperator &IRCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                        PhysicalOperator &plan) {
	throw NotImplementedException("ICCatalog PlanUpdate");
}
unique_ptr<LogicalOperator> IRCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("ICCatalog BindCreateIndex");
}

bool IRCatalog::HasCachedValue(string url) const {
	auto value = metadata_cache.find(url);
	if (value != metadata_cache.end()) {
		auto now = std::chrono::system_clock::now();
		if (now < value->second->expires_at) {
			return true;
		}
	}
	return false;
}

string IRCatalog::GetCachedValue(string url) const {
	auto value = metadata_cache.find(url);
	if (value != metadata_cache.end()) {
		auto now = std::chrono::system_clock::now();
		if (now < value->second->expires_at) {
			return value->second->data;
		}
	}
	throw InternalException("Cached value does not exist");
}

//! FIXME: this always returns false???
bool IRCatalog::SetCachedValue(string url, const string &value, const rest_api_objects::LoadTableResult &result) {
	//! FIXME: shouldn't this also store the 'storage-credentials' ??
	if (!result.has_config) {
		return false;
	}
	auto &credentials = result.config;
	auto expires_at_it = credentials.find("s3.session-token-expires-at-ms");
	if (expires_at_it == credentials.end()) {
		return false;
	}

	auto &expires_at = expires_at_it->second;
	auto epochMillis = std::stoll(expires_at.c_str());
	auto expired_time = std::chrono::system_clock::time_point(std::chrono::milliseconds(epochMillis));
	auto val = make_uniq<MetadataCacheValue>(value, expired_time);
	metadata_cache[url] = std::move(val);
	return false;
}

} // namespace duckdb
