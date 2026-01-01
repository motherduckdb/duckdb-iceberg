
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

struct TableTransactionInfo {
	TableTransactionInfo() {};

	rest_api_objects::CommitTransactionRequest request;
	// if a table is created with assert create, we cannot use the
	// transactions/commit endpoint. Instead we iterate through each table
	// update and update each table individually
	bool has_assert_create = false;
};

class IRCTransaction : public Transaction {
public:
	IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IRCTransaction() override;

public:
	void Start();
	void Commit();
	void Rollback();
	static IRCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}
	void DoTableUpdates(ClientContext &context);
	void DoTableDeletes(ClientContext &context);
	bool DirtyTablesHaveUpdates();
	IRCatalog &GetCatalog();
	void DropSecrets(ClientContext &context);
	TableTransactionInfo GetTransactionRequest(ClientContext &context);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IRCatalog &catalog;
	AccessMode access_mode;

public:
	//! tables that have been created in this transaction
	//! tables are hashed by catalog_name.table name
	//! Tables that have been updated in this transaction, to be rewritten on commit.
	case_insensitive_map_t<IcebergTableInformation> updated_tables;
	//! tables that have been deleted in this transaction, to be deleted on commit.
	case_insensitive_map_t<IcebergTableInformation> deleted_tables;
	//! Tables that have been requested in the current transaction
	//! and do not need to be requested again. When we request, we also
	//! store the latest snapshot id, so if the table is requested again
	//! (with no updates), we can return table information at that snapshot
	//! while other transactions can still request up to date tables
	case_insensitive_map_t<idx_t> requested_tables;

	case_insensitive_set_t created_secrets;
	case_insensitive_set_t looked_up_entries;
};

} // namespace duckdb
