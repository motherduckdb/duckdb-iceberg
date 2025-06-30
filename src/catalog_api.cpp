#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/irc_catalog.hpp"
#include "yyjson.hpp"
#include "iceberg_utils.hpp"
#include "api_utils.hpp"
#include <sys/stat.h>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

[[noreturn]] static void ThrowException(const string &url, const HTTPResponse &response, const string &method) {
	D_ASSERT(!response.Success());

	if (response.HasRequestError()) {
		//! Request error - this means something went wrong performing the request
		throw IOException("%s request to endpoint '%s' failed: (ERROR %s)", method, url, response.GetRequestError());
	}
	//! FIXME: the spec defines response objects for all failure conditions, we can deserialize the response and
	//! return a more descriptive error message based on that.

	//! If this was not a request error this means the server responded - report the response status and response
	throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n)", method, url,
	                    int(response.status));
}

static string GetTableMetadata(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table);

	auto url = url_builder.GetURL();
	auto response = catalog.auth_handler->GetRequest(context, url_builder);
	if (!response->Success()) {
		ThrowException(url, *response, "GET");
	}

	const auto &api_result = response->body;
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);
	catalog.SetCachedValue(url, api_result, load_table_result);
	return api_result;
}

vector<string> IRCAPI::GetCatalogs(ClientContext &context, IRCatalog &catalog) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

rest_api_objects::LoadTableResult IRCAPI::GetTable(ClientContext &context, IRCatalog &catalog, const string &schema,
                                                   const string &table_name) {
	string result = GetTableMetadata(context, catalog, schema, table_name);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(result));
	auto *metadata_root = yyjson_doc_get_root(doc.get());
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(metadata_root);
	return load_table_result;
}

// TODO: handle out-of-order columns using position property
vector<rest_api_objects::TableIdentifier> IRCAPI::GetTables(ClientContext &context, IRCatalog &catalog,
                                                            const string &schema) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema);
	url_builder.AddPathComponent("tables");
	auto response = catalog.auth_handler->GetRequest(context, url_builder);
	if (!response->Success()) {
		auto url = url_builder.GetURL();
		ThrowException(url, *response, "GET");
	}

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
	auto *root = yyjson_doc_get_root(doc.get());
	auto list_tables_response = rest_api_objects::ListTablesResponse::FromJSON(root);

	if (!list_tables_response.has_identifiers) {
		throw NotImplementedException("List of 'identifiers' is missing, missing support for Iceberg V1");
	}
	return std::move(list_tables_response.identifiers);
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, IRCatalog &catalog) {
	vector<IRCAPISchema> result;
	auto endpoint_builder = catalog.GetBaseUrl();
	endpoint_builder.AddPathComponent(catalog.prefix);
	endpoint_builder.AddPathComponent("namespaces");
	auto response = catalog.auth_handler->GetRequest(context, endpoint_builder);
	if (!response->Success()) {
		auto url = endpoint_builder.GetURL();
		ThrowException(url, *response, "GET");
	}

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
	auto *root = yyjson_doc_get_root(doc.get());
	auto list_namespaces_response = rest_api_objects::ListNamespacesResponse::FromJSON(root);
	if (!list_namespaces_response.has_namespaces) {
		//! FIXME: old code expected 'namespaces' to always be present, but it's not a required property
		return result;
	}
	auto &schemas = list_namespaces_response.namespaces;
	for (auto &schema : schemas) {
		IRCAPISchema schema_result;
		schema_result.catalog_name = catalog.GetName();
		auto &value = schema.value;
		if (value.size() != 1) {
			//! FIXME: we likely want to fix this by concatenating the components with a `.` ?
			throw NotImplementedException("Only a namespace with a single component is supported currently, found %d",
			                              value.size());
		}
		schema_result.schema_name = value[0];
		result.push_back(schema_result);
	}

	return result;
}

} // namespace duckdb
