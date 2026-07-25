#include "execution/operator/iceberg_delete.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

//! Equality-delete write helpers. The functions defined here are only invoked when the
//! ICEBERG_ENABLE_EQUALITY_DELETE_WRITES compile flag is on - in default builds the callers
//! (in iceberg_delete.cpp) are #ifdef'd out, so this code is dead.

static bool PlanContainsPhysicalFilter(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::FILTER) {
		return true;
	}
	for (auto &child : plan.children) {
		if (PlanContainsPhysicalFilter(child.get())) {
			return true;
		}
	}
	return false;
}

namespace {

static bool TryGetColumnPath(const Expression &expr, vector<Identifier> &column_path);

static bool TryGetFunctionColumnPath(const BoundFunctionExpression &function, vector<Identifier> &column_path) {
	idx_t child_index;
	if (!TryGetStructExtractChildIndex(function, child_index) || function.GetChildren().empty()) {
		return false;
	}
	auto &base = *function.GetChildren()[0];
	if (!TryGetColumnPath(base, column_path) || base.GetReturnType().id() != LogicalTypeId::STRUCT ||
	    child_index >= StructType::GetChildCount(base.GetReturnType())) {
		return false;
	}
	//! Recurse into the base first so nested extracts produce a root-to-leaf field path.
	column_path.push_back(StructType::GetChildName(base.GetReturnType(), child_index));
	return true;
}

static bool TryGetOperatorColumnPath(const BoundOperatorExpression &struct_extract, vector<Identifier> &column_path) {
	auto &children = struct_extract.GetChildren();
	if (children.size() != 2 || children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &field_value = children[1]->Cast<BoundConstantExpression>().GetValue();
	if (field_value.IsNull() || field_value.type().id() != LogicalTypeId::VARCHAR ||
	    !TryGetColumnPath(*children[0], column_path)) {
		return false;
	}

	auto remaining_fields = QualifiedName::ParseComponents(field_value.GetValue<string>());
	//! A STRUCT_EXTRACT operator can encode more than one remaining field in its constant.
	for (auto &field : remaining_fields) {
		column_path.push_back(std::move(field));
	}
	return !remaining_fields.empty();
}

static bool TryGetColumnPath(const Expression &expr, vector<Identifier> &column_path) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_REF:
	case ExpressionType::BOUND_COLUMN_REF:
		return true;
	case ExpressionType::STRUCT_EXTRACT:
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			return TryGetOperatorColumnPath(expr.Cast<BoundOperatorExpression>(), column_path);
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			return TryGetFunctionColumnPath(expr.Cast<BoundFunctionExpression>(), column_path);
		}
		return false;
	case ExpressionType::BOUND_FUNCTION:
		return TryGetFunctionColumnPath(expr.Cast<BoundFunctionExpression>(), column_path);
	default:
		return false;
	}
}

} // namespace

bool IcebergDelete::TryGetEqualityDeletePredicates(ClientContext &context, IcebergTableEntry &table,
                                                   PhysicalOperator &child_plan,
                                                   vector<IcebergEqualityDeletePredicate> &equality_predicates) {
	//! Gated behind an explicit testing-only setting.
	Value setting_value;
	if (!context.TryGetCurrentSetting(ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE, setting_value) ||
	    setting_value.IsNull() || !setting_value.GetValue<bool>()) {
		return false;
	}

	//! Equality-delete writing is only supported for v2, unpartitioned tables.
	auto &table_metadata = table.table_info.table_metadata;
	if (table_metadata.iceberg_version != 2) {
		return false;
	}
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		return false;
	}

	//! Any filter means this cannot be an equality delete.
	if (PlanContainsPhysicalFilter(child_plan)) {
		return false;
	}

	auto table_scan = FindIcebergScan(child_plan);
	if (!table_scan) {
		return false;
	}
	auto &scan = *table_scan;
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return false;
	}

	auto &schema = table_metadata.GetLatestSchema();
	auto &columns = schema.columns;
	for (auto &filter_entry : *scan.table_filters) {
		auto column_key = filter_entry.GetIndex().GetIndex();
		auto &table_filter = filter_entry.Filter().Cast<ExpressionFilter>();
		auto &expr = *table_filter.expr;

		//! Only a plain `column = constant` qualifies (rejects IN, OR/AND conjunctions, IS NULL, ...).

		if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			return false;
		}
		if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		auto &compare_expr = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(compare_expr);
		auto &right = BoundComparisonExpression::Right(compare_expr);

		optional_ptr<const Value> constant_value;
		optional_ptr<const Expression> column_expression;
		if (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			column_expression = left;
			constant_value = right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			column_expression = right;
			constant_value = left.Cast<BoundConstantExpression>().GetValue();
		} else {
			return false;
		}
		vector<Identifier> column_path;
		if (!TryGetColumnPath(*column_expression, column_path)) {
			return false;
		}

		if (column_key >= scan.column_ids.size()) {
			return false;
		}
		auto &column_index = scan.column_ids[column_key];
		if (column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= columns.size()) {
			return false;
		}
		optional_ptr<const IcebergColumnDefinition> column_definition = columns[primary_index].get();
		for (auto &child_name : column_path) {
			column_definition = column_definition->GetChild(child_name.GetIdentifierName());
			if (!column_definition) {
				return false;
			}
		}
		//! The same column referenced more than once is not a clean equality delete.
		for (auto &existing : equality_predicates) {
			if (existing.field_id == column_definition->id) {
				return false;
			}
		}
		Value delete_value;
		string error_message;
		if (!constant_value->DefaultTryCastAs(column_definition->type, delete_value, &error_message, true)) {
			return false;
		}
		IcebergEqualityDeletePredicate predicate;
		predicate.field_id = column_definition->id;
		predicate.column_name = column_definition->name;
		predicate.type = column_definition->type;
		predicate.value = std::move(delete_value);
		equality_predicates.push_back(std::move(predicate));
	}
	return !equality_predicates.empty();
}

void IcebergDelete::WriteEqualityDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state) const {
	D_ASSERT(!equality_predicates.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = table.table_info.table_metadata.GetDataPath(fs);
	string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-equality-deletes.parquet";
	string delete_file_path = fs.JoinPath(data_path, delete_filename);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// Generate the field ids for the parquet writer: every column carries, as PARQUET:field_id
	// metadata, the iceberg field-id that the equality delete applies to.
	child_list_t<Value> field_id_values;
	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	vector<int32_t> equality_ids;
	for (auto &predicate : equality_predicates) {
		field_id_values.emplace_back(predicate.column_name, Value::INTEGER(predicate.field_id));
		names_to_write.push_back(predicate.column_name);
		types_to_write.push_back(predicate.type);
		equality_ids.push_back(predicate.field_id);
	}
	vector<Value> field_input;
	field_input.push_back(Value::STRUCT(std::move(field_id_values)));
	info->options["field_ids"] = std::move(field_input);

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data =
	    copy_fun.function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// Write a single row containing the equality-delete tuple (one value per equality column).
	DataChunk write_chunk;
	write_chunk.Initialize(context, types_to_write);
	for (idx_t col_idx = 0; col_idx < equality_predicates.size(); col_idx++) {
		write_chunk.data[col_idx].SetValue(0, equality_predicates[col_idx].value);
	}
	write_chunk.SetChildCardinality(1);
	copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
	                               write_chunk);

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	IcebergDeleteFileInfo delete_file;
	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = 1;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.equality_ids = std::move(equality_ids);
	global_state.written_files.emplace(delete_file_path, std::move(delete_file));
}

} // namespace duckdb
