#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

struct IcebergPhysicalCopy : public PhysicalOperator {
public:
	IcebergPhysicalCopy(vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
	}

	// Keep table information alive throughout execution
	IcebergTableInformation table_info;
	shared_ptr<IcebergTableSchema> table_schema;

public:
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
};

struct IcebergLogicalCopy : public LogicalExtensionOperator {
public:
	IcebergLogicalCopy() : LogicalExtensionOperator() {
	}

	unique_ptr<FunctionData> bind_data;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner);

private:
	unique_ptr<IcebergTableMetadata> CreateTableMetadata(ClientContext &context, const vector<LogicalType> &types,
	                                                     const vector<string> &names, FunctionData &bind_data);
	shared_ptr<IcebergTableSchema> CreateTableSchema(ClientContext &context, const vector<LogicalType> &types,
	                                                 const vector<string> &names);
};

} // namespace duckdb
