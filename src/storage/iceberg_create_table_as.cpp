#include "storage/iceberg_create_table_as.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/iceberg_insert.hpp"
#include "storage/irc_table_entry.hpp"
#include "iceberg_multi_file_list.hpp"
#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

IcebergCreateTableAs::IcebergCreateTableAs(LogicalOperator &op, unique_ptr<BoundCreateTableInfo> info, Catalog &catalog)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), schema(nullptr), info(std::move(info)),
      catalog(catalog) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class IcebergCreateTableAsGlobalState : public GlobalSinkState {
public:
	explicit IcebergCreateTableAsGlobalState() = default;
	// does a create table need any manifest files?
	vector<IcebergManifestEntry> written_files;
};

unique_ptr<GlobalSinkState> IcebergCreateTableAs::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergCreateTableAsGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

SinkResultType IcebergCreateTableAs::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergCreateTableAsGlobalState>();

	// TODO: pass through the partition id?
	// here we copy the files yea?
	// ?? Not sure, this could honestly be a NOP.

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergCreateTableAs::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergCreateTableAsGlobalState>();
	auto value = Value::BIGINT(1);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType IcebergCreateTableAs::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergCreateTableAsGlobalState>();

	// create the create table request in the transaction
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto &transaction = IRCTransaction::Get(context, ic_catalog);

	//	table_info.AddSnapshot(transaction, std::move(global_state.written_files));
	// here I need to figure out a way to add the proper information to the transaction
	//	auto createTableRequest = make_uniq<IcebergCreateTableRequest>(context, *info);
	//	transaction.AddCreateTableRequest(irc_table);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergCreateTableAs::GetName() const {
	return "Iceberg Create Table";
}

InsertionOrderPreservingMap<string> IcebergCreateTableAs::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = "boop";
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, name)->Cast<CopyFunctionCatalogEntry>();
}

static Value GetFieldIdValue(const IcebergColumnDefinition &column) {
	auto column_value = Value::BIGINT(column.id);
	if (column.children.empty()) {
		// primitive type - return the field-id directly
		return column_value;
	}
	// nested type - generate a struct and recurse into children
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(column_value));
	for (auto &child : column.children) {
		values.emplace_back(child->name, GetFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

static Value WrittenFieldIds(const IcebergTableSchema &schema) {
	auto &columns = schema.columns;

	child_list_t<Value> values;
	for (idx_t c_idx = 0; c_idx < columns.size(); c_idx++) {
		auto &column = columns[c_idx];
		values.emplace_back(column->name, GetFieldIdValue(*column));
	}
	return Value::STRUCT(std::move(values));
}

PhysicalOperator &IRCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &create_info = op.info->Base();

	// TODO: check if create_info contains partitioned information, if yes, error
	// if (create_info.partition_info) {
	// 		return InvalidInputException("creating partitioned tables not yet supported");
	// }

	auto transaction = CatalogTransaction::GetSystemTransaction(*context.db);
	auto &schema = op.schema;

	auto &ic_schema_entry = schema.Cast<IRCSchemaEntry>();
	auto &catalog = ic_schema_entry.catalog;
	auto &irc_transaction = IRCTransaction::Get(context, catalog);

	// create the table
	auto table = ic_schema_entry.CreateTable(irc_transaction, context, *op.info);
	auto &ic_table = table->Cast<ICTableEntry>();
	auto &table_schema = ic_table.table_info->table_metadata.GetLatestSchema();

	// Create Copy Info
	IcebergCopyInput copy_input(context, ic_table);
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(table_schema));
	copy_input.options["field_ids"] = std::move(field_input);

	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, plan);
	physical_index_vector_t<idx_t> column_index_map;
	auto &insert = planner.Make<IcebergInsert>(op, ic_table, column_index_map);

	//	return planner.Make<IcebergInsert>(op, the_schema.get(), std::move(create_info));
	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
