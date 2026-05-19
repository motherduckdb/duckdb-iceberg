#include "planning/iceberg_optimizer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

GuaranteeEqualityDeleteColumnsOptimizer::GuaranteeEqualityDeleteColumnsOptimizer(ClientContext &context)
    : context(context) {
}

void GuaranteeEqualityDeleteColumnsOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto &child = op->children[child_index];
		if (child->type != LogicalOperatorType::LOGICAL_GET) {
			VisitOperator(child);
			return;
		}
		auto &get = child->Cast<LogicalGet>();
		if (get.function.name != "iceberg_scan" || !get.bind_data) {
			VisitOperator(child);
			return;
		}
		auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
		if (!mfbd.file_list) {
			return;
		}
		auto &iceberg_list = mfbd.file_list->Cast<IcebergMultiFileList>();
		{
			lock_guard<mutex> guard(iceberg_list.delete_lock);
			iceberg_list.EnumerateDeleteManifestEntries();
		}

		unordered_set<int32_t> required_field_ids;
		for (auto &entry : iceberg_list.delete_manifest_entries) {
			auto &mft = entry.entry;
			if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
				continue;
			}
			for (auto fid : mft.data_file.equality_ids) {
				required_field_ids.insert(fid);
			}
		}

		if (required_field_ids.empty()) {
			return;
		}

		auto &schema_columns = iceberg_list.GetSchema().columns;
		vector<unique_ptr<Expression>> args;
		for (auto fid : required_field_ids) {
			idx_t schema_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < schema_columns.size(); i++) {
				if (schema_columns[i]->id == fid) {
					schema_idx = i;
					break;
				}
			}
			if (schema_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			idx_t local_idx = DConstants::INVALID_INDEX;
			const auto &col_ids = get.GetColumnIds();
			for (idx_t i = 0; i < col_ids.size(); i++) {
				if (!col_ids[i].IsVirtualColumn() && col_ids[i].GetPrimaryIndex() == schema_idx) {
					local_idx = i;
					break;
				}
			}
			if (local_idx == DConstants::INVALID_INDEX) {
				get.AddColumnId(schema_idx);
				local_idx = get.GetColumnIds().size() - 1;
			}
			auto bindings = get.GetColumnBindings();
			args.push_back(make_uniq<BoundColumnRefExpression>(schema_columns[schema_idx]->type, bindings[local_idx]));
		}
		if (args.empty()) {
			return;
		}

		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &fn_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "iceberg_verify_equality_deletes");
		FunctionBinder function_binder(context);
		vector<LogicalType> arg_types;
		for (auto &a : args) {
			arg_types.push_back(a->return_type);
		}
		auto fn = fn_entry.functions.GetFunctionByArguments(context, arg_types);
		auto bound_call = function_binder.BindScalarFunction(fn, std::move(args));

		auto filter = make_uniq<LogicalFilter>();
		filter->expressions.push_back(std::move(bound_call));
		filter->children.push_back(std::move(op->children[child_index]));
		op->children[child_index] = std::move(filter);
	}
}

void IcebergOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	GuaranteeEqualityDeleteColumnsOptimizer guarantee_equality_delete_columns_optimizer(input.context);
	if (plan->children.size() == 0) {
		return;
	}
	guarantee_equality_delete_columns_optimizer.VisitOperator(plan);
	plan->Print();
}

OptimizerExtension IcebergOptimizer::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergOptimizer::PreOptimize;
	return ext;
}

} // namespace duckdb
