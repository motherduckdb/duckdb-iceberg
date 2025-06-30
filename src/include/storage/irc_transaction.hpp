
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

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
	IRCSchemaSet &GetSchemas() {
		return schemas;
	}
	void MarkTableAsDirty(const ICTableEntry &table);
	void AddCreateTableRequest(unique_ptr<IcebergCreateTableRequest> creat_table_request);
	IRCatalog &GetCatalog();
	void CreateEntry(unique_ptr<ICTableEntry> entry);
	void CommitNewTables(ClientContext &context);
	void DropSecrets(ClientContext &context);
	rest_api_objects::CommitTransactionRequest GetTransactionRequest(ClientContext &context);

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IRCatalog &catalog;
	AccessMode access_mode;

public:
	IRCSchemaSet schemas;
	//! Tables marked dirty in this transaction, to be rewritten on commit
	unordered_set<const ICTableEntry *> dirty_tables;
	vector<unique_ptr<ICTableEntry>> new_tables;
	case_insensitive_set_t created_secrets;
};

} // namespace duckdb
