//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class IcebergOptimizerRoutine {
public:
	ClientContext &context;

public:
	IcebergOptimizerRoutine(ClientContext &context);
	void VisitOperator(unique_ptr<LogicalOperator> &op);

private:
	void VisitOperator(unique_ptr<LogicalOperator> &op, bool below_write);
};

class IcebergOptimizer {
public:
	static OptimizerExtension Create();
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
