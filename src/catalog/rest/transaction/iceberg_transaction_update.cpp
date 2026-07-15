#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

IcebergTransactionAlterUpdate::IcebergTransactionAlterUpdate(IcebergTransaction &transaction)
    : transaction(transaction) {
}
IcebergTransactionAlterUpdate::~IcebergTransactionAlterUpdate() {
}

void IcebergTransactionAlterUpdate::CheckWriteWriteConflict(const IcebergTableInformation &table_info) const {
	auto &iceberg_transaction = transaction;
	auto context_ptr = iceberg_transaction.context.lock();
	auto &context = *context_ptr;

	auto table_key = table_info.GetTableKey();
	auto transaction_start = IcebergUtils::GetTransactionStartTimeMS(context);
	auto &metadata = table_info.table_metadata;

	auto table_state = iceberg_transaction.GetLatestTableState(table_key);
	if (table_state && table_state->GetInfo().HasTransactionUpdates()) {
		//! Already has transaction changes, no need to verify further
		return;
	}

	auto latest_snapshot = metadata.GetSnapshotByTimestampMS(transaction_start);

	if (transaction_start < metadata.last_updated_ms &&
	    (!latest_snapshot || latest_snapshot->GetSchemaId() != metadata.GetCurrentSchemaId())) {
		throw TransactionException(
		    "Write-write conflict detected on '%s', changes were made since the start of the transaction", table_key);
	}

	auto committed_snapshot = metadata.GetLatestCommittedSnapshot();
	if (latest_snapshot != committed_snapshot) {
		throw TransactionException(
		    "Write-write conflict detected on '%s', changes were made since the start of the transaction", table_key);
	}
}

IcebergTableInformation &IcebergTransactionAlterUpdate::GetOrInitializeTable(const IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto it = updated_tables.find(table_key);
	if (it == updated_tables.end()) {
		auto state = transaction.GetLatestTableState(table_key);
		CheckWriteWriteConflict(state ? state->GetCatalogInfo() : table);
		auto &transaction_state = state ? *state : transaction.GetOrCreateTransactionTableState(table);
		auto &updated_table = transaction_state.GetOrCreateTransactionInfo(transaction);
		it = updated_tables.emplace(table_key, updated_table).first;
		transaction.VerifyAlterUpdateAtomicity(*this);
	}
	auto &result = it->second.get();
	transaction.SetLatestTableState(table_key, IcebergTableStatus::ALIVE);
	return result;
}

bool IcebergTransactionAlterUpdate::HasUpdates() const {
	for (auto &it : updated_tables) {
		auto &table = it.second.get();
		if (table.HasTransactionUpdates()) {
			return true;
		}
	}
	return false;
}

IcebergTableInformation &IcebergTransactionAlterUpdate::CreateTable(const string &table_key,
                                                                    IcebergTableInformation &&table) {
	auto &state = transaction.SetTransactionTableState(table_key, std::move(table), IcebergTableStatus::ALIVE);
	auto &created_table = state.GetInfo();
	auto emplace_res = updated_tables.emplace(table_key, created_table);
	if (!emplace_res.second) {
		throw InternalException("Table %s was already created somehow?", table_key);
	}
	transaction.VerifyAlterUpdateAtomicity(*this);
	return created_table;
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               IcebergTableInformation &table)
    : transaction(transaction), deleted_table(table) {
}
IcebergTransactionDeleteUpdate::~IcebergTransactionDeleteUpdate() {
}

IcebergTransactionRenameUpdate::IcebergTransactionRenameUpdate(IcebergTransaction &transaction,
                                                               IcebergTableInformation &table,
                                                               IcebergTableInformation &new_table,
                                                               const string &new_name)
    : transaction(transaction), table(table), new_table(new_table), new_name(new_name) {
}
IcebergTransactionRenameUpdate::~IcebergTransactionRenameUpdate() {
}

} // namespace duckdb
