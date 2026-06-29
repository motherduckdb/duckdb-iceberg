#include "catalog/rest/storage/iceberg_table_secret_provider.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

const char *IcebergTableSecretProvider::Provider() {
	return "iceberg";
}

static Value RefreshInfoToStruct(const Value &refresh_info) {
	if (refresh_info.type().id() == LogicalTypeId::STRUCT) {
		return refresh_info;
	}
	if (refresh_info.type().id() != LogicalTypeId::MAP) {
		throw InvalidInputException("Invalid input passed to refresh_info");
	}

	child_list_t<Value> struct_fields;
	for (const auto &kv_child : MapValue::GetChildren(refresh_info)) {
		auto kv_pair = StructValue::GetChildren(kv_child);
		if (kv_pair.size() != 2) {
			throw InvalidInputException("Invalid input passed to refresh_info");
		}
		struct_fields.emplace_back(kv_pair[0].ToString(), kv_pair[1]);
	}
	return Value::STRUCT(std::move(struct_fields));
}

static unique_ptr<BaseSecret> BuildVendedSecret(CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	secret->redact_keys = {"secret", "session_token", "bearer_token", "http_proxy_password"};

	if (input.type == "r2") {
		auto account_id_entry = input.options.find("account_id");
		if (account_id_entry != input.options.end()) {
			secret->secret_map["endpoint"] = account_id_entry->second.ToString() + ".r2.cloudflarestorage.com";
			secret->secret_map["url_style"] = "path";
		}
	}

	for (auto &option : input.options) {
		auto key = StringUtil::Lower(option.first);
		if (key == "account_id") {
			continue;
		}
		if (key == "catalog_name" || key == "schema" || key == "table") {
			continue;
		}
		if (key == "refresh_info") {
			secret->secret_map[key] = RefreshInfoToStruct(option.second);
			continue;
		}
		secret->secret_map[key] = option.second;
	}
	return std::move(secret);
}

static string GetRequiredRefreshOption(const CreateSecretInput &input, const string &key) {
	auto entry = input.options.find(key);
	if (entry == input.options.end() || entry->second.IsNull()) {
		throw InvalidInputException("Missing '%s' in Iceberg vended credential refresh_info", key);
	}
	return entry->second.ToString();
}

static unique_ptr<SecretEntry> GetHTTPSecretForCatalog(ClientContext &context, IcebergCatalog &catalog) {
	switch (catalog.auth_handler->type) {
	case IcebergAuthorizationType::SIGV4: {
		auto &sigv4 = catalog.auth_handler->Cast<SIGV4Authorization>();
		return IcebergCatalog::GetHTTPSecret(context, sigv4.secret);
	}
	case IcebergAuthorizationType::OAUTH2:
		return IcebergCatalog::GetHTTPSecret(context, "");
	default:
		return nullptr;
	}
}

static CreateSecretInput ReVendVendedCredentials(ClientContext &context, CreateSecretInput &input) {
	auto catalog_name = GetRequiredRefreshOption(input, "catalog_name");
	auto schema_name = GetRequiredRefreshOption(input, "schema");
	auto table_name = GetRequiredRefreshOption(input, "table");

	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();

	auto schema_entry = ic_catalog.GetSchemas().GetEntry(context, schema_name, OnEntryNotFound::THROW_EXCEPTION);
	auto &iceberg_schema = schema_entry->Cast<IcebergSchemaEntry>();

	auto get_table_result = IRCAPI::GetTable(context, ic_catalog, iceberg_schema, table_name);
	if (get_table_result.has_error) {
		throw HTTPException(StringUtil::Format("Could not refresh Iceberg vended credentials for table '%s': "
		                                       "GetTableInformation returned response code %s with message \"%s\"",
		                                       table_name, EnumUtil::ToString(get_table_result.status_),
		                                       get_table_result.error_._error.message));
	}

	auto table_info = IcebergTableInformation(ic_catalog, iceberg_schema, table_name);
	table_info.InitializeFromLoadTableResult(*get_table_result.result_);
	auto credentials =
	    table_info.GetVendedCredentials(context, VendedCredentialsSecretTracking::SKIP_CREATED_SECRETS);

	optional_ptr<CreateSecretInput> match;
	if (credentials.config) {
		match = credentials.config.get();
	} else {
		for (auto &candidate : credentials.storage_credentials) {
			if (candidate.scope == input.scope) {
				match = &candidate;
				break;
			}
		}
		if (!match && credentials.storage_credentials.size() == 1) {
			match = &credentials.storage_credentials[0];
		}
	}
	if (!match) {
		throw InvalidConfigurationException("Could not refresh Iceberg vended credentials for table '%s': no "
		                                    "matching "
		                                    "credential was re-vended",
		                                    table_name);
	}

	auto result = std::move(*match);
	result.name = input.name;
	result.scope = input.scope;
	result.type = input.type;
	result.provider = input.provider;
	result.storage_type = input.storage_type;
	result.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	result.persist_type = SecretPersistType::TEMPORARY;

	auto http_secret_entry = GetHTTPSecretForCatalog(context, ic_catalog);
	if (http_secret_entry) {
		AddHTTPSecretsToOptions(*http_secret_entry, result.options);
	}
	return result;
}

bool IcebergTableSecretProvider::SupportsStorageType(const string &storage_type) {
	return StringUtil::CIEquals(storage_type, "s3") || StringUtil::CIEquals(storage_type, "gcs") ||
	       StringUtil::CIEquals(storage_type, "r2");
}

unique_ptr<BaseSecret> IcebergTableSecretProvider::CreateSecret(ClientContext &context, CreateSecretInput &input) {
	if (input.options.find("catalog_name") != input.options.end()) {
		DUCKDB_LOG_INFO(context, "Refreshing Iceberg vended credentials for secret '%s'", input.name);
		auto revended = ReVendVendedCredentials(context, input);
		revended.options["refreshed_secret"] = Value("true");
		return BuildVendedSecret(revended);
	}
	return BuildVendedSecret(input);
}

Value IcebergTableSecretProvider::MakeRefreshInfo(const string &catalog_name, const string &schema_name,
                                                  const string &table_name) {
	child_list_t<Value> fields;
	fields.emplace_back("catalog_name", Value(catalog_name));
	fields.emplace_back("schema", Value(schema_name));
	fields.emplace_back("table", Value(table_name));
	return Value::STRUCT(std::move(fields));
}

void IcebergTableSecretProvider::Register(ExtensionLoader &loader) {
	for (const char *type : {"s3", "gcs", "r2"}) {
		CreateSecretFunction function = {type, Provider(), CreateSecret};

		function.named_parameters["refresh_info"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		function.named_parameters["catalog_name"] = LogicalType::VARCHAR;
		function.named_parameters["schema"] = LogicalType::VARCHAR;
		function.named_parameters["table"] = LogicalType::VARCHAR;

		function.named_parameters["key_id"] = LogicalType::VARCHAR;
		function.named_parameters["secret"] = LogicalType::VARCHAR;
		function.named_parameters["session_token"] = LogicalType::VARCHAR;
		function.named_parameters["region"] = LogicalType::VARCHAR;
		function.named_parameters["endpoint"] = LogicalType::VARCHAR;
		function.named_parameters["url_style"] = LogicalType::VARCHAR;
		function.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
		function.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
		function.named_parameters["kms_key_id"] = LogicalType::VARCHAR;
		function.named_parameters["url_compatibility_mode"] = LogicalType::BOOLEAN;
		function.named_parameters["requester_pays"] = LogicalType::BOOLEAN;
		function.named_parameters["bearer_token"] = LogicalType::VARCHAR;
		function.named_parameters["account_id"] = LogicalType::VARCHAR;
		function.named_parameters["expires_at"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy_username"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy_password"] = LogicalType::VARCHAR;
		function.named_parameters["extra_http_headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

		function.named_parameters["refreshed_secret"] = LogicalType::VARCHAR;

		loader.RegisterFunction(function);
	}
}

} // namespace duckdb
