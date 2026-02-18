#include "storage/iceberg_table_update.hpp"
#include "storage/iceberg_transaction_data.hpp"

namespace duckdb {

IcebergCommitState::IcebergCommitState(IcebergTransactionData &transaction_data) : transaction_data(transaction_data) {
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type, IcebergTableInformation &table_info)
    : type(type), table_info(table_info) {
}

} // namespace duckdb
