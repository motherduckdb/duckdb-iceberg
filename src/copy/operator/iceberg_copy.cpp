#include "copy/operator/iceberg_copy.hpp"

namespace duckdb {

PhysicalOperator &IcebergLogicalCopy::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	// Plan the child (the SELECT query)
	auto &child_plan = planner.CreatePlan(*children[0]);

	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	// Create a BoundCreateTableInfo from the query output
	auto create_info = make_uniq<CreateTableInfo>();
	create_info->table = "temp_iceberg_table"; // temporary name, not used for local tables

	// Add columns from the query output
	for (idx_t i = 0; i < child_plan.names.size(); i++) {
		create_info->columns.AddColumn(ColumnDefinition(child_plan.names[i], child_plan.types[i]));
	}

	// Use IcebergCreateTableRequest::CreateIcebergSchema to create the schema
	// We need a temporary IcebergTableEntry for this
	auto table_info = CreateTableInformation(context, *create_info, copy_bind_data);
	auto table_entry = CreateTableEntry(context, table_info, *create_info);

	auto table_schema = IcebergCreateTableRequest::CreateIcebergSchema(context, *table_entry);

	// Create IcebergCopyInput with the necessary metadata
	IcebergCopyInput copy_input(context, table_info.table_metadata, *table_schema);

	// Create a parquet copy operator as the child
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &child_plan);

	// Create the IcebergPhysicalCopy operator and store the metadata
	auto &iceberg_copy = planner.Make<IcebergPhysicalCopy>(types, estimated_cardinality);
	iceberg_copy.table_info = std::move(table_info);
	iceberg_copy.table_schema = std::move(table_schema);
	iceberg_copy.children.push_back(physical_copy);

	return iceberg_copy;
}

SinkResultType IcebergPhysicalCopy::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CopyIcebergGlobalState>();
	auto &lstate = input.local_state.Cast<CopyIcebergLocalState>();

	// Write data files (similar to IcebergInsert)
	// Write to parquet files in data/ subdirectory
	// Track manifest entries

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<IcebergTableMetadata> IcebergLogicalCopy::CreateTableMetadata(ClientContext &context,
                                                                         const vector<LogicalType> &types,
                                                                         const vector<string> &names,
                                                                         FunctionData &bind_data) {
	auto &copy_bind_data = bind_data.Cast<CopyIcebergBindData>();

	auto metadata = make_uniq<IcebergTableMetadata>();
	metadata->table_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	metadata->location = copy_bind_data.file_path;
	metadata->iceberg_version = 2; // Use format version 2
	metadata->current_schema_id = 0;
	metadata->default_spec_id = 0;
	metadata->last_updated_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	        .count();

	return metadata;
}

shared_ptr<IcebergTableSchema> IcebergLogicalCopy::CreateTableSchema(ClientContext &context,
                                                                     const vector<LogicalType> &types,
                                                                     const vector<string> &names) {
	auto schema = make_shared_ptr<IcebergTableSchema>();
	schema->schema_id = 0;

	idx_t field_id = 1;
	for (idx_t i = 0; i < types.size(); i++) {
		auto column = make_uniq<IcebergColumnDefinition>();
		column->name = names[i];
		column->type = types[i];
		column->field_id = field_id++;
		column->required = false;
		schema->columns.push_back(std::move(column));
	}

	return schema;
}

} // namespace duckdb
