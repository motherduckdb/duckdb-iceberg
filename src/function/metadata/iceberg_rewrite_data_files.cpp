#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/rewrite_data_files_executor.hpp"
#include "maintenance/rewrite_data_files_operator.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"
#include "maintenance/table_identifier.hpp"

namespace duckdb {

namespace {

struct RewriteDataFilesOptions {
	MaintenanceTableKey table_key;
	int64_t target_file_size_bytes = 134217728;
	int64_t min_input_files = 5;
	bool rewrite_all = false;
};

static RewriteDataFilesOptions ParseOptions(TableFunctionBindInput &input) {
	RewriteDataFilesOptions result;
	result.table_key = ParseMaintenanceTableIdentifier("iceberg_rewrite_data_files", StringValue::Get(input.inputs[0]));

	for (auto &kv : input.named_parameters) {
		auto opt = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if (opt == "strategy") {
			auto strategy = StringValue::Get(val);
			if (strategy != "binpack") {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: only 'binpack' strategy is supported, got '%s'", strategy);
			}
		} else if (opt == "target_file_size_bytes") {
			auto value = val.GetValue<int64_t>();
			if (value <= 0) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'target_file_size_bytes' must be > 0, got %lld", value);
			}
			result.target_file_size_bytes = value;
		} else if (opt == "min_input_files") {
			auto value = val.GetValue<int64_t>();
			if (value < 1) {
				throw InvalidInputException("iceberg_rewrite_data_files: 'min_input_files' must be >= 1, got %lld",
				                            value);
			}
			result.min_input_files = value;
		} else if (opt == "rewrite_all") {
			result.rewrite_all = BooleanValue::Get(val);
		}
	}
	return result;
}

static unique_ptr<QueryNode> BuildGroupSelect(const MaintenanceTableKey &table_key,
                                              const vector<RewriteCandidate> &group) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());

	auto table = make_uniq<BaseTableRef>();
	table->catalog_name = table_key.catalog;
	table->schema_name = table_key.schema;
	table->table_name = table_key.table;
	table->alias = "rewrite_source";
	select->from_table = std::move(table);

	vector<unique_ptr<ParsedExpression>> in_children;
	in_children.push_back(make_uniq<ColumnRefExpression>("filename"));
	for (auto &candidate : group) {
		in_children.push_back(make_uniq<ConstantExpression>(Value(candidate.file_path)));
	}
	select->where_clause = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
	return std::move(select);
}

static unique_ptr<LogicalOperator> BindGroupCopy(Binder &binder, const RewritePlan &plan,
                                                 const vector<RewriteCandidate> &group, idx_t group_idx) {
	auto &metadata = plan.table_info->table_metadata;
	auto schema_id = metadata.GetCurrentSchemaId();
	auto schema_it = metadata.GetSchemas().find(schema_id);
	if (schema_it == metadata.GetSchemas().end()) {
		throw InternalException("iceberg_rewrite_data_files: current schema id %d not found in metadata", schema_id);
	}

	auto &fs = FileSystem::GetFileSystem(binder.context);
	CopyStatement copy_statement;
	copy_statement.info->select_statement = BuildGroupSelect(plan.table_key, group);
	copy_statement.info->file_path =
	    fs.JoinPath(metadata.GetDataPath(fs), StringUtil::Format("rewrite_group_%llu", group_idx));
	copy_statement.info->format = "parquet";
	copy_statement.info->is_from = false;
	copy_statement.info->is_format_auto_detected = false;
	copy_statement.info->options["field_ids"].push_back(BuildRewriteFieldIds(*schema_it->second));
	copy_statement.info->options["filename_pattern"].push_back(Value("{uuidv7}"));
	copy_statement.info->options["return_files"].push_back(Value::BOOLEAN(true));
	copy_statement.info->options["per_thread_output"].push_back(Value::BOOLEAN(false));
	copy_statement.info->options["overwrite_or_ignore"].push_back(Value::BOOLEAN(true));

	auto copy_binder = Binder::CreateBinder(binder.context, &binder);
	auto bound_copy = copy_binder->Bind(copy_statement);
	if (bound_copy.types.size() != 2) {
		throw InternalException("iceberg_rewrite_data_files: expected COPY RETURN_FILES to return two columns");
	}
	return std::move(bound_copy.plan);
}

static unique_ptr<LogicalOperator> RewriteDataFilesBindOperator(ClientContext &context, TableFunctionBindInput &input,
                                                                idx_t bind_index, vector<string> &return_names) {
	if (!input.binder) {
		throw InternalException("iceberg_rewrite_data_files: bind_operator called without a binder");
	}
	auto options = ParseOptions(input);
	input.binder->SetAlwaysRequireRebind();

	RewriteDataFilesPlanInput plan_input;
	plan_input.table_key = options.table_key;
	plan_input.target_file_size_bytes = options.target_file_size_bytes;
	plan_input.min_input_files = options.min_input_files;
	plan_input.rewrite_all = options.rewrite_all;
	auto plan = PlanRewrite(context, plan_input);

	auto result = make_uniq<LogicalRewriteDataFiles>(bind_index, std::move(plan));
	for (idx_t group_idx = 0; group_idx < result->plan.file_groups.size(); group_idx++) {
		result->children.push_back(
		    BindGroupCopy(*input.binder, result->plan, result->plan.file_groups[group_idx], group_idx));
	}

	return_names = {"rewritten_data_files", "added_data_files", "rewritten_bytes"};
	return std::move(result);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRewriteDataFilesFunction() {
	TableFunctionSet function_set("iceberg_rewrite_data_files");
	TableFunction function({LogicalType::VARCHAR}, nullptr);
	function.bind_operator = RewriteDataFilesBindOperator;
	function.named_parameters["strategy"] = LogicalType::VARCHAR;
	function.named_parameters["target_file_size_bytes"] = LogicalType::BIGINT;
	function.named_parameters["min_input_files"] = LogicalType::BIGINT;
	function.named_parameters["rewrite_all"] = LogicalType::BOOLEAN;
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
