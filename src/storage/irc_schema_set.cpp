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

	auto &irc_transaction = IRCTransaction::Get(context, catalog);

	auto verify_existence = irc_transaction.looked_up_entries.insert(name).second;
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	if (!verify_existence) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Iceberg namespace by the name of '%s' does not exist", name);
	}
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
	// By getting the IRC transaction here we initialize it, which will
	// allow us to clear the entries at the end of a transaction
	auto &irc_transaction = IRCTransaction::Get(context, catalog);
}

void IRCSchemaSet::AddEntry(string name, unique_ptr<IRCSchemaEntry> entry) {
	entries.insert(make_pair(name, std::move(entry)));
}

CatalogEntry &IRCSchemaSet::GetEntry(const string &name) {
	auto exists = entries.find(name) != entries.end();
	if (!exists) {
		throw CatalogException("Schema '%s' does not exist", name);
	}
	auto &entry = entries.find(name)->second;
	return *entry;
}

const case_insensitive_map_t<unique_ptr<CatalogEntry>> &IRCSchemaSet::GetEntries() {
	return entries;
}

void IRCSchemaSet::ClearEntries() {
	for (auto &entry : entries) {
		auto &ic_schema_entry = entry.second.get()->Cast<IRCSchemaEntry>();
		ic_schema_entry.ClearTableEntries();
	}
	listed = false;
}

static string GetSchemaName(const vector<string> &items) {
	return StringUtil::Join(items, ".");
}

void IRCSchemaSet::LoadEntries(ClientContext &context) {
	if (listed) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog, {});
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = GetSchemaName(schema.items);
		info.internal = false;
		auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
		schema_entry->namespace_items = std::move(schema.items);
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
