#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "rest_catalog/objects/list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/storage/iceberg_table_secret_provider.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {
class OAuth2Authorization;
constexpr column_t IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER;

static void DropTemporaryIcebergSecrets(ClientContext &context, vector<string> &secret_names) {
	if (secret_names.empty()) {
		return;
	}

	bool started_transaction = false;
	try {
		if (!context.transaction.HasActiveTransaction()) {
			context.transaction.BeginTransaction();
			started_transaction = true;
		}

		auto &secret_manager = SecretManager::Get(context);
		for (auto &secret_name : secret_names) {
			secret_manager.DropSecretByName(context, secret_name, OnEntryNotFound::RETURN_NULL,
			                                SecretPersistType::TEMPORARY, SecretManager::TEMPORARY_STORAGE_NAME);
		}

		if (started_transaction) {
			context.transaction.Commit();
		}
	} catch (...) {
		if (started_transaction && context.transaction.HasActiveTransaction()) {
			context.transaction.Rollback(nullptr);
		}
		// Query-end cleanup must not mask the query result.
	}
	secret_names.clear();
}

class IcebergTemporarySecretCleanupState : public ClientContextState {
public:
	void TrackSecret(string secret_name) {
		lock_guard<mutex> guard(lock);
		temporary_secret_names.push_back(std::move(secret_name));
	}

	void QueryBegin(ClientContext &context) override {
		lock_guard<mutex> guard(lock);
		temporary_secret_names.clear();
	}

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		vector<string> secret_names;
		{
			lock_guard<mutex> guard(lock);
			secret_names.swap(temporary_secret_names);
		}
		DropTemporaryIcebergSecrets(context, secret_names);
	}

private:
	mutex lock;
	vector<string> temporary_secret_names;
};

static void TrackTemporarySecretForQuery(ClientContext &context, const string &secret_name) {
	if (!context.transaction.HasActiveTransaction() || context.transaction.GetActiveQuery() == MAXIMUM_QUERY_ID) {
		return;
	}
	auto state =
	    context.registered_state->GetOrCreate<IcebergTemporarySecretCleanupState>("iceberg_temporary_secret_cleanup");
	state->TrackSecret(secret_name);
}

IcebergTableEntry::IcebergTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
                                     CreateTableInfo &info, optional_idx schema_id)
    : TableCatalogEntry(catalog, schema, info), table_info(table_info), schema_id(schema_id) {
	this->internal = false;
}

unique_ptr<BaseStatistics> IcebergTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void IcebergTableEntry::PrepareIcebergScanFromEntry(ClientContext &context) const {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &secret_manager = SecretManager::Get(context);

	if (ic_catalog.attach_options.access_mode != IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		// assume secret already exists
		return;
	}
	// Get Credentials from IRC API
	auto &fs = FileSystem::GetFileSystem(context);
	auto table_credentials = table_info.GetVendedCredentials(context);
	auto metadata_path = table_info.table_metadata.GetMetadataPath(fs);
	auto &transaction = IcebergTransaction::Get(context, ic_catalog);

	auto http_secret_entry = IcebergTableSecretProvider::GetHTTPSecretForCatalog(context, ic_catalog);

	if (table_credentials.config) {
		auto &info = *table_credentials.config;
		D_ASSERT(info.scope.empty());
		string storage_scope;
		auto data_path = table_info.table_metadata.table_properties.find("write.data.path");
		if (data_path != table_info.table_metadata.table_properties.end()) {
			storage_scope = data_path->second;
		} else {
			storage_scope = table_info.table_metadata.GetLocation();
		}
		if (!storage_scope.empty()) {
			if (!StringUtil::EndsWith(storage_scope, "/")) {
				storage_scope += "/";
			}
			info.scope = {storage_scope};
		} else {
			string lc_storage_location = StringUtil::Lower(metadata_path);
			size_t metadata_pos = lc_storage_location.find("metadata");
			if (metadata_pos != string::npos) {
				info.scope = {metadata_path.substr(0, metadata_pos)};
			} else {
				DUCKDB_LOG_INFO(context,
				                "Creating Iceberg Table secret with no scope. Returned metadata location is %s",
				                lc_storage_location);
			}
		}

		if (StringUtil::StartsWith(ic_catalog.uri, "glue")) {
			auto &sigv4 = ic_catalog.auth_handler->Cast<SIGV4Authorization>();
			auto secret_entry = IcebergCatalog::GetStorageSecret(context, sigv4.secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

			//! Override the endpoint if 'glue' is the host of the catalog
			auto region = kv_secret.TryGetValue("region").ToString();
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options["endpoint"] = endpoint;
		} else if (StringUtil::StartsWith(ic_catalog.uri, "s3tables")) {
			auto &sigv4 = ic_catalog.auth_handler->Cast<SIGV4Authorization>();
			auto secret_entry = IcebergCatalog::GetStorageSecret(context, sigv4.secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

			//! Override all the options if 's3tables' is the host of the catalog
			auto substrings = StringUtil::Split(ic_catalog.GetWarehouse(), ":");
			D_ASSERT(substrings.size() == 6);
			auto region = substrings[3];
			auto endpoint = "s3." + region + ".amazonaws.com";

			info.options = {{"key_id", kv_secret.TryGetValue("key_id").ToString()},
			                {"secret", kv_secret.TryGetValue("secret").ToString()},
			                {"session_token", kv_secret.TryGetValue("session_token").IsNull()
			                                      ? ""
			                                      : kv_secret.TryGetValue("session_token").ToString()},
			                {"region", region},
			                {"endpoint", endpoint}};
		}

		if (http_secret_entry) {
			IcebergTableSecretProvider::AddHTTPSecretsToOptions(*http_secret_entry, info.options);
		}

		auto created_secret = secret_manager.CreateSecret(context, info);
		auto secret_name = created_secret->secret->GetName();
		transaction.created_secrets.insert(secret_name);
		TrackTemporarySecretForQuery(context, secret_name);
		// if there is no key_id, secret, token (S3/GCS) or account_name, connection_string (Azure) in the info,
		// log that vended credentials has not worked
		bool has_s3_creds = info.options.find("key_id") != info.options.end() ||
		                    info.options.find("secret") != info.options.end() ||
		                    info.options.find("token") != info.options.end();
		bool has_azure_creds = info.options.find("account_name") != info.options.end() ||
		                       info.options.find("connection_string") != info.options.end();
		bool has_gcs_creds = info.options.find("bearer_token") != info.options.end();
		if (!has_s3_creds && !has_azure_creds && !has_gcs_creds) {
			DUCKDB_LOG_INFO(context, "Failed to create valid secret from Vended Credentials for table '%s'",
			                table_info.name);
		}
	} else {
		for (auto &info : table_credentials.storage_credentials) {
			if (http_secret_entry) {
				IcebergTableSecretProvider::AddHTTPSecretsToOptions(*http_secret_entry, info.options);
			}
			auto created_secret = secret_manager.CreateSecret(context, info);
			auto secret_name = created_secret->secret->GetName();
			transaction.created_secrets.insert(secret_name);
			TrackTemporarySecretForQuery(context, secret_name);
		}
	}
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                 const EntryLookupInfo &lookup) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "iceberg_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"iceberg_scan\" not found!");
	}
	auto &iceberg_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto iceberg_scan_function =
	    iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	PrepareIcebergScanFromEntry(context);

	if (!schema_id.IsValid()) {
		throw InternalException("GetScanFunction was called with a dummy IcebergTableEntry, this should never happen");
	}
	const auto schema_id = this->schema_id.GetIndex();
	const auto &metadata = table_info.table_metadata;
	const auto &iceberg_schema = *metadata.GetSchemaFromId(schema_id);

	// lookup should be asof start of the transaction if the lookup info is empty and there are no transaction updates
	bool using_transaction_timestamp = false;
	IcebergSnapshotLookup snapshot_lookup;
	if (!lookup.GetAtClause() && !table_info.HasTransactionUpdates()) {
		// if there is no user supplied AT () clause, and the table does not have transaction updates
		// use transaction start time
		snapshot_lookup = table_info.GetSnapshotLookup(context);
		using_transaction_timestamp = true;
	} else {
		auto at = lookup.GetAtClause();
		snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	}

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info = metadata.GetSnapshot(snapshot_lookup);
	//! Override whatever schema id the lookup resulted in
	//! The schema is preset by the IcebergCatalogEntry and we can not deviate from that
	snapshot_info.schema_id = schema_id;

	if (!metadata.snapshots.empty() && !snapshot_info.snapshot && using_transaction_timestamp) {
		// We are using the transaction start time.
		// The table is not empty, but GetSnapshot is asking for table state before the first snapshot
		// table creation has no snapshot, so we return this error message
		throw InvalidConfigurationException("Table %s does not have a reachable state in this transaction",
		                                    table_info.GetTableKey());
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto scan_info =
	    make_shared_ptr<IcebergScanInfo>(metadata.GetMetadataPath(fs), metadata, snapshot_info, iceberg_schema);
	if (table_info.transaction_data && snapshot_lookup.IsLatest()) {
		scan_info->transaction_data = table_info.transaction_data.get();
	}

	iceberg_scan_function.function_info = scan_info;
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	const auto &storage_location = metadata.location;
	vector<Value> inputs = {storage_location};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	auto result = iceberg_scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);
	auto &file_bind_data = bind_data->Cast<MultiFileBindData>();
	file_bind_data.virtual_columns = GetVirtualColumns();
	D_ASSERT(file_bind_data.file_list);
	auto &ic_file_list = file_bind_data.file_list->Cast<IcebergMultiFileList>();
	ic_file_list.SetTable(this);
	return iceberg_scan_function;
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("IcebergTableEntry::GetScanFunction called without entry lookup info");
}

virtual_column_map_t IcebergTableEntry::GetVirtualColumns() const {
	return VirtualColumns();
}

virtual_column_map_t IcebergTableEntry::VirtualColumns() {
	virtual_column_map_t result;
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	result.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("_row_id", LogicalType::BIGINT));
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	               TableColumn("file_row_number", LogicalType::BIGINT));
	result.emplace(IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER,
	               TableColumn("_last_updated_sequence_number", LogicalType::BIGINT));
	return result;
}

vector<column_t> IcebergTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	auto &table_metadata = table_info.table_metadata;
	if (table_metadata.iceberg_version >= 3) {
		//! Project the _row_id column as part of the row-id-columns
		result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableStorageInfo IcebergTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

string IcebergTableEntry::GetUUID() const {
	return table_info.table_id;
}

} // namespace duckdb
