#include "planning/iceberg_optimizer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {


void GuaranteeEqualityDeleteColumnsOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		if (child->type != LogicalOperatorType::LOGICAL_GET) {
			VisitOperator(child);
		}
		auto &get = child->Cast<LogicalGet>();
		if (get.function.name != "iceberg_scan" || !get.bind_data) {
			VisitOperator(child);
		}
		auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
		if (!mfbd.file_list) {
			return;
		}
		auto &iceberg_list = mfbd.file_list->Cast<IcebergMultiFileList>();
	}
}

void IcebergOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	GuaranteeEqualityDeleteColumnsOptimizer guarantee_equality_delete_columns_optimizer;
	D_ASSERT(plan->children.size() == 1);
	guarantee_equality_delete_columns_optimizer.VisitOperator(plan->children[0]);
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::Optimize;
	return ext;
}

}