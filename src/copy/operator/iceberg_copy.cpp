#include "copy/operator/iceberg_copy.hpp"

namespace duckdb {

PhysicalOperator &IcebergLogicalCopy::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	// Plan the child (the SELECT query)
	auto &child_plan = planner.CreatePlan(*children[0]);

	// Create IcebergCopyInput with the necessary metadata
	// This will need to be stored in the logical operator during binding
	IcebergCopyInput copy_input(context, table_entry, schema);

	// Create a parquet copy operator as the child
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, child_plan);

	// Create the IcebergPhysicalCopy operator
	auto &iceberg_copy = planner.Make<IcebergPhysicalCopy>(types, estimated_cardinality);
	iceberg_copy.children.push_back(physical_copy);

	return iceberg_copy;
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
