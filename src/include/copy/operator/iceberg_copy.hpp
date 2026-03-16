#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

struct IcebergPhysicalCopy : public PhysicalOperator {
public:
	IcebergPhysicalCopy(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
	}

	// Keep bind data alive throughout execution (contains metadata and schema)
	unique_ptr<FunctionData> bind_data;

public:
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
};

struct IcebergLogicalCopy : public LogicalExtensionOperator {
public:
	IcebergLogicalCopy() : LogicalExtensionOperator() {
	}

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner);
	void ResolveTypes();

public:
	unique_ptr<FunctionData> bind_data;
};

} // namespace duckdb
