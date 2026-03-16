#include "copy/operator/iceberg_copy.hpp"
#include "copy/function/iceberg_copy_function.hpp"
#include "storage/iceberg_insert.hpp"

namespace duckdb {

void IcebergLogicalCopy::ResolveTypes() {
	types = {LogicalType::BIGINT};
}

PhysicalOperator &IcebergLogicalCopy::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	// Plan the child (the SELECT query)
	auto &child_plan = planner.CreatePlan(*children[0]);

	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	// Create IcebergCopyInput with the metadata from bind data
	IcebergCopyInput copy_input(context, *copy_bind_data.table_metadata, *copy_bind_data.table_schema);

	// Create a parquet copy operator as the child
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &child_plan);

	// Create the IcebergPhysicalCopy operator and move bind_data to keep metadata alive
	auto &op = planner.Make<IcebergPhysicalCopy>(types, estimated_cardinality);
	auto &iceberg_copy = op.Cast<IcebergPhysicalCopy>();
	iceberg_copy.bind_data = std::move(bind_data);
	iceberg_copy.children.push_back(physical_copy);

	return op;
}

SinkResultType IcebergPhysicalCopy::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CopyIcebergGlobalState>();
	auto &lstate = input.local_state.Cast<CopyIcebergLocalState>();

	// Write data files (similar to IcebergInsert)
	// Write to parquet files in data/ subdirectory
	// Track manifest entries

	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
