#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_set.hpp"
#include "storage/irc_transaction.hpp"

namespace duckdb {

IRCSchemaSet::IRCSchemaSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> IRCSchemaSet::GetEntry(ClientContext &context, const string &name,
                                                  OnEntryNotFound if_not_found) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto entry = entries.find(name);
	if (entry == entries.end()) {
		CreateSchemaInfo info;
		if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, name)) {
			if (if_not_found == OnEntryNotFound::RETURN_NULL) {
				return nullptr;
			} else {
				throw CatalogException("Iceberg namespace by the name of '%s' does not exist", name);
			}
		}
		info.schema = name;
		info.internal = false;
		auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
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
	if (listed) {
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
	listed = true;
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
