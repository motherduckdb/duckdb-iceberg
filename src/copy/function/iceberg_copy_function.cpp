#include "copy/function/iceberg_copy_function.hpp"
#include "copy/operator/iceberg_copy.hpp"

namespace duckdb {

static BoundStatement IcebergCopyPlan(Binder &binder, CopyStatement &stmt) {
	auto &select_stmt = stmt.select_statement->Cast<SelectStatement>();
	auto query = binder.Bind(*select_stmt);

	// Create logical copy operator
	auto logical_copy = make_uniq<IcebergLogicalCopy>();
	logical_copy->children.push_back(std::move(query));

	BoundStatement result;
	result.types = {LogicalType::BIGINT}; // return rows written
	result.names = {"Count"};
	result.plan = std::move(logical_copy);
	return result;
}

CopyFunction IcebergCopyFunction::Create() {
	auto res = CopyFunction("iceberg");
	res.plan = IcebergCopyPlan;
	return res;
}

} // namespace duckdb
