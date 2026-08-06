
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

namespace duckdb {
struct CreateTableInfo;
class IcebergSchemaEntry;
class IcebergTransaction;

class IcebergTableSet {
public:
	explicit IcebergTableSet(IcebergSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	static IcebergTable &CreateNewEntry(ClientContext &context, IcebergCatalog &catalog, IcebergSchemaEntry &schema,
	                                    CreateTableInfo &info);
	shared_ptr<IcebergTable> CreateEntryInternal(lock_guard<mutex> &guard, const string &name, IcebergTable &&table,
	                                             shared_ptr<IcebergTable> &old_entry);
	const case_insensitive_map_t<shared_ptr<IcebergTable>> &GetEntries();
	case_insensitive_map_t<shared_ptr<IcebergTable>> &GetEntriesMutable();
	mutex &GetEntryLock();

private:
	IcebergTableSchemaVersion &GetOrCreateDummy(IcebergTable &table_info) const;

public:
	void LoadEntries(ClientContext &context);
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTable &table);

public:
	IcebergSchemaEntry &schema;
	Catalog &catalog;

private:
	case_insensitive_map_t<shared_ptr<IcebergTable>> entries;
	mutex entry_lock;
};

} // namespace duckdb
