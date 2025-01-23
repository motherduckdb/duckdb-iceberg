#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/ic_catalog.hpp"
#include "storage/ic_table_set.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/ic_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

IBTableSet::IBTableSet(IBSchemaEntry &schema) : IBInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, IBAPIColumnDefinition &coldef) {
	return {coldef.name, IBUtils::TypeToLogicalType(context, coldef.type_text)};
}

unique_ptr<CatalogEntry> IBTableSet::_CreateCatalogEntry(ClientContext &context, IBAPITable table) {
	D_ASSERT(schema.name == table.schema_name);
	CreateTableInfo info;
	info.table = table.name;

	for (auto &col : table.columns) {
		info.columns.AddColumn(CreateColumnDefinition(context, col));
	}

	auto table_entry = make_uniq<IBTableEntry>(catalog, schema, info);
	table_entry->table_data = make_uniq<IBAPITable>(table);
	return table_entry;
}

void IBTableSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	auto* derived = static_cast<IBTableEntry*>(entry.get());
	if (!derived->table_data->storage_location.empty()) {
		return;
	}
		
	auto &ic_catalog = catalog.Cast<IBCatalog>();
	auto table = IBAPI::GetTable(catalog.GetName(), catalog.GetDBPath(), schema.name, entry->name, ic_catalog.credentials);
	entry = _CreateCatalogEntry(context, table);
}

void IBTableSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IBCatalog>();
	// TODO: handle out-of-order columns using position property
	auto tables = IBAPI::GetTables(catalog.GetName(), catalog.GetDBPath(), schema.name, ic_catalog.credentials);

	for (auto &table : tables) {
		auto entry = _CreateCatalogEntry(context, table);
		CreateEntry(std::move(entry));
	}
}

optional_ptr<CatalogEntry> IBTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<IBTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<IBTableInfo> IBTableSet::GetTableInfo(ClientContext &context, IBSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("IBTableSet::CreateTable");
}

optional_ptr<CatalogEntry> IBTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	throw NotImplementedException("IBTableSet::CreateTable");
}

void IBTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("IBTableSet::AlterTable");
}

void IBTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("IBTableSet::AlterTable");
}

void IBTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("IBTableSet::AlterTable");
}

void IBTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("IBTableSet::AlterTable");
}

void IBTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("IBTableSet::AlterTable");
}

} // namespace duckdb
