#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_set.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/irc_context_state.hpp"

namespace duckdb {

IRCSchemaSet::IRCSchemaSet(Catalog &catalog) : catalog(catalog) {
}

void IRCSchemaSet::VerifySchemas(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	for (auto &entry : entries) {
		auto &schema = entry.second->Cast<IRCSchemaEntry>();
		if (schema.existence_state.type != SchemaExistenceType::UNKNOWN) {
			continue;
		}
		if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, schema.name)) {
			throw CatalogException("Iceberg namespace by the name of '%s' does not exist", schema.name);
		}
		schema.existence_state.type = SchemaExistenceType::PRESENT;
	}
}

optional_ptr<CatalogEntry> IRCSchemaSet::GetEntry(ClientContext &context, const string &name,
                                                  OnEntryNotFound if_not_found) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto entry = entries.find(name);
	if (entry == entries.end()) {
		auto context_state = context.registered_state->GetOrCreate<IRCContextState>("iceberg");

		//! We create the entry immediately optimistically,
		//! when we scan from the table we'll figure out if it exists or not.
		CreateSchemaInfo info;
		info.schema = name;
		info.internal = false;
		auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
		context_state->RegisterSchema(*schema_entry);
		schema_entry->existence_state.if_not_found = if_not_found;
		CreateEntryInternal(context, std::move(schema_entry));
		entry = entries.find(name);
		D_ASSERT(entry != entries.end());
	}
	return entry->second.get();
}

void IRCSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

void IRCSchemaSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog);
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema;
		info.internal = false;
		auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
		CreateEntryInternal(context, std::move(schema_entry));
	}
}

optional_ptr<CatalogEntry> IRCSchemaSet::CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("IRCSchemaSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

} // namespace duckdb
