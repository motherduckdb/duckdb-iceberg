#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_utils.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/iceberg_table_information.hpp"
#include "metadata/iceberg_table_metadata.hpp"

#include <string>
#include <numeric>

namespace duckdb {

struct SetIcebergTablePropertiesBindData : public TableFunctionData {
	optional_ptr<ICTableEntry> iceberg_table;
	vector<string> properties;
};

struct SetIcebergTablePropertiesGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	SetIcebergTablePropertiesGlobalTableFunctionState() {};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<SetIcebergTablePropertiesGlobalTableFunctionState>();
	}

	idx_t property_count = 0;
};

static unique_ptr<FunctionData> IcebergTablePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<SetIcebergTablePropertiesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto properties = input.inputs[1].ToString();
	ret->properties.push_back(properties);
	auto iceberg_table = IcebergUtils::GetICTableEntry(context, input_string);
	ret->iceberg_table = iceberg_table;

	return_types.insert(return_types.end(), LogicalType::BIGINT);
	names.insert(names.end(), string("Success"));
	return std::move(ret);
}

static unique_ptr<FunctionData> GetIcebergTablePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<SetIcebergTablePropertiesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto iceberg_table = IcebergUtils::GetICTableEntry(context, input_string);
	ret->iceberg_table = iceberg_table;

	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	return_types.insert(return_types.end(), LogicalType::VARCHAR);
	names.insert(names.end(), string("key"));
	names.insert(names.end(), string("value"));
	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void SetIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &irc_transaction = IRCTransaction::Get(context, iceberg_table->catalog);
	if (!iceberg_table->table_info.transaction_data) {
		iceberg_table->table_info.transaction_data =
		    make_uniq<IcebergTransactionData>(context, iceberg_table->table_info);
	}
	IcebergTransactionData &transaction_data = *(iceberg_table->table_info.transaction_data);

	auto schema = iceberg_table->schema.name;
	auto table_name = iceberg_table->name;
	case_insensitive_map_t<string> properties;
	properties.emplace("write.update.mode", "merge-on-read");
	transaction_data.TableSetProperties(properties);
	irc_transaction.dirty_tables.emplace(iceberg_table.get());

	// set success output, failure happens during transaction commit.
	AddString(output.data[0], 0, string_t("true"));
	output.SetCardinality(1);
}

static void RemoveIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &irc_transaction = IRCTransaction::Get(context, iceberg_table->catalog);
	if (!iceberg_table->table_info.transaction_data) {
		iceberg_table->table_info.transaction_data =
		    make_uniq<IcebergTransactionData>(context, iceberg_table->table_info);
	}
	IcebergTransactionData &transaction_data = *(iceberg_table->table_info.transaction_data);

	auto schema = iceberg_table->schema.name;
	auto table_name = iceberg_table->name;
	vector<string> properties;
	properties.push_back("write.update.mode");
	transaction_data.TableRemoveProperties(properties);
	irc_transaction.dirty_tables.emplace(iceberg_table.get());

	// set success output, failure happens during transaction commit.
	AddString(output.data[0], 0, string_t("true"));
	output.SetCardinality(1);
}

static void GetIcebergTablePropertiesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SetIcebergTablePropertiesBindData>();
	auto &global_state = data.global_state->Cast<SetIcebergTablePropertiesGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}
	auto iceberg_table = bind_data.iceberg_table;
	auto &table_info = iceberg_table->table_info;
	if (!table_info.load_table_result.metadata.has_properties) {
		output.SetCardinality(0);
		return;
	}
	// if we have already returned all properties.
	if (global_state.property_count >= table_info.load_table_result.metadata.properties.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = global_state.property_count;
	for (auto &property : table_info.load_table_result.metadata.properties) {
		AddString(output.data[0], count, string_t(property.first));
		AddString(output.data[1], count, string_t(property.second));
		count++;
	}

	global_state.property_count += count;
	output.SetCardinality(count);
}

TableFunctionSet IcebergFunctions::SetIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("set_iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, SetIcebergTablePropertiesFunction,
	                         IcebergTablePropertiesBind, SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::RemoveIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("remove_iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, RemoveIcebergTablePropertiesFunction,
	                         IcebergTablePropertiesBind, SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

TableFunctionSet IcebergFunctions::GetIcebergTablePropertiesFunctions() {
	TableFunctionSet function_set("iceberg_table_properties");

	auto fun = TableFunction({LogicalType::VARCHAR}, GetIcebergTablePropertiesFunction, GetIcebergTablePropertiesBind,
	                         SetIcebergTablePropertiesGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
