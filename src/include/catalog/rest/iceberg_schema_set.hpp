
#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include "catalog_entry/schema/iceberg_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class IcebergSchemaSet {
public:
	explicit IcebergSchemaSet(Catalog &catalog);

public:
	void LoadEntries(ClientContext &context);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	vector<shared_ptr<IcebergSchemaEntry>> GetEntries(ClientContext &context);
	void AddEntry(const string &name, shared_ptr<IcebergSchemaEntry> entry);
	void RemoveEntry(const string &name);

protected:
	void LoadEntriesInternal(ClientContext &context);
	shared_ptr<IcebergSchemaEntry> CreateEntryInternal(shared_ptr<IcebergSchemaEntry> entry);

public:
	Catalog &catalog;

private:
	case_insensitive_map_t<shared_ptr<IcebergSchemaEntry>> entries;
	mutex entry_lock;
};

} // namespace duckdb
