#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

struct IcebergPhysicalCopy : public PhysicalOperator {
public:
	IcebergPhysicalCopy() {
	}

public:
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
};

struct IcebergLogicalCopy : public LogicalExtensionOperator {
public:
	IcebergLogicalCopy() {
	}

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner);

public:
};

} // namespace duckdb
