#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

namespace {

static IcebergTableInformation CopyLatestState(IcebergTransaction &transaction, const IcebergTableInformation &table) {
	auto key = table.GetTableKey();
	auto state = transaction.GetLatestTableState(key);
	if (!state) {
		throw InternalException("No transaction state exists for table with key '%s'", key);
	}
	return state->GetInfo().Copy();
}

} // namespace

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
	if (table_state->GetInfo().HasTransactionUpdates()) {
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
		CheckWriteWriteConflict(table);
		it = updated_tables.emplace(table_key, CopyLatestState(transaction, table)).first;
		transaction.VerifyAlterUpdateAtomicity(*this);
		// Preserve the table_uuid from the original table info (resolved at transaction start).
		// Copy() reads from the global request cache, which can be contaminated by another
		// transaction's RENAME overwriting the entry with a different table's metadata.
		// Only override when the original has a known UUID (skip for newly created tables).
		if (!table.table_metadata.table_uuid.empty()) {
			it->second.table_metadata.table_uuid = table.table_metadata.table_uuid;
		}
		it->second.InitSchemaVersions();
	}
	transaction.SetLatestTableState(it->second, IcebergTableStatus::ALIVE);
	return it->second;
}

bool IcebergTransactionAlterUpdate::HasUpdates() const {
	for (auto &it : updated_tables) {
		auto &table = it.second;
		if (table.HasTransactionUpdates()) {
			return true;
		}
	}
	return false;
}

IcebergTableInformation &IcebergTransactionAlterUpdate::CreateTable(const string &table_key,
                                                                    IcebergTableInformation &&table) {
	auto emplace_res = updated_tables.emplace(table_key, std::move(table));
	if (!emplace_res.second) {
		throw InternalException("Table %s was already created somehow?", table_key);
	}
	transaction.VerifyAlterUpdateAtomicity(*this);

	transaction.current_table_data.emplace(table_key, IcebergTransactionTableState(emplace_res.first->second));
	return emplace_res.first->second;
}

IcebergTransactionDeleteUpdate::IcebergTransactionDeleteUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table)
    : transaction(transaction), deleted_table(table.Copy(transaction)) {
}
IcebergTransactionDeleteUpdate::~IcebergTransactionDeleteUpdate() {
}

IcebergTransactionRenameUpdate::IcebergTransactionRenameUpdate(IcebergTransaction &transaction,
                                                               const IcebergTableInformation &table,
                                                               const string &new_name)
    : transaction(transaction), table(table), new_table(CopyLatestState(transaction, table)), new_name(new_name) {
	new_table.name = new_name;
	if (!table.schema_versions.empty()) {
		new_table.InitSchemaVersions();
	}
}
IcebergTransactionRenameUpdate::~IcebergTransactionRenameUpdate() {
}

} // namespace duckdb
