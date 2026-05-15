//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class GuaranteeEqualityDeleteColumnsOptimizer {
public:
	void VisitOperator(unique_ptr<LogicalOperator> &op);
};

class IcebergOptimizer {
public:
	static OptimizerExtension Create();
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

}