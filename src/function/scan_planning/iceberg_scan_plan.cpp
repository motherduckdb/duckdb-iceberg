#include "function/iceberg_functions.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "core/metadata/partition/iceberg_partition_constants.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "planning/scan_plan/iceberg_scan_planner.hpp"
#include "function/scan_planning/iceberg_scan_task_codec.hpp"

namespace duckdb {

struct IcebergScanPlanBindData : public TableFunctionData {
	IcebergScanPlanBindData(IcebergTableSchemaVersion &table, shared_ptr<IcebergScanInfo> scan_info,
	                        const IcebergOptions &options)
	    : table(table), scan_info(std::move(scan_info)), options(options) {
	}

	IcebergTableSchemaVersion &table;
	shared_ptr<IcebergScanInfo> scan_info;
	IcebergOptions options;
	bool produce_sequence_number = false;
	LogicalType partition_type;
};

struct IcebergScanPlanGlobalState : public GlobalTableFunctionState {
	explicit IcebergScanPlanGlobalState(ClientContext &context, const IcebergScanPlanBindData &bind)
	    : planner(context, bind.scan_info, bind.scan_info->metadata.location, bind.options),
	      metadata(LogicalType::VARIANT(), 1) {
		// Vended credentials are transaction-scoped, so recreate them on every execution.
		bind.table.PrepareIcebergScanFromEntry(context);
		planner.SetTable(bind.table);
		if (bind.produce_sequence_number) {
			// The server planning API does not yet provide file sequence numbers.
			planner.DisableServerSidePlanning();
		}
		Vector json(LogicalType::JSON(), 1);
		json.SetValue(0, Value(bind.scan_info->metadata.ToJSON()));
		VectorOperations::Cast(context, json, metadata, 1);
		metadata.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	IcebergScanPlanner planner;
	Vector metadata;
	idx_t file_id = 0;
	bool done = false;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergScanPlanGlobalState>(context, input.bind_data->Cast<IcebergScanPlanBindData>());
	}
};

static unique_ptr<FunctionData> IcebergScanPlanBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("Expected fully qualified table name (catalog.schema.table), got NULL");
	}
	auto input_string = input.inputs[0].ToString();
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	if (qualified_name.size() != 3) {
		throw InvalidInputException("Expected fully qualified table name (catalog.schema.table), got: %s",
		                            input_string);
	}
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY,
	                             QualifiedName(qualified_name[0], qualified_name[1], qualified_name[2]));
	auto catalog_entry = Catalog::GetEntry(context, table_lookup, OnEntryNotFound::THROW_EXCEPTION);
	if (catalog_entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("'%s' is not a table", input_string);
	}
	auto &table = catalog_entry->Cast<TableCatalogEntry>();
	if (table.catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("Table '%s' is not an Iceberg REST catalog table", input_string);
	}
	for (const auto &parameter : input.named_parameters) {
		if (parameter.second.IsNull()) {
			if (parameter.first == "produce_sequence_number") {
				throw InvalidInputException("iceberg_scan_plan produce_sequence_number cannot be NULL");
			}
			throw InvalidInputException("iceberg_scan_plan snapshot arguments cannot be NULL");
		}
	}
	IcebergOptions options(input.named_parameters);
	auto &table_entry = table.Cast<IcebergTableSchemaVersion>();
	auto &metadata = table_entry.table_info.table_metadata;
	auto snapshot = metadata.GetSnapshot(*options.snapshot_lookup);
	auto &schema = metadata.GetSchemaFromId(snapshot.schema_id);
	auto &fs = FileSystem::GetFileSystem(context);
	auto scan_info = make_shared_ptr<IcebergScanInfo>(metadata.GetMetadataPath(fs), metadata, snapshot, schema);
	if (options.snapshot_lookup->IsLatest() && table_entry.table_info.transaction_data) {
		scan_info->transaction_data = table_entry.table_info.transaction_data.get();
	}
	auto ret = make_uniq<IcebergScanPlanBindData>(table_entry, std::move(scan_info), options);
	auto produce_sequence_number = input.named_parameters.find("produce_sequence_number");
	if (produce_sequence_number != input.named_parameters.end()) {
		ret->produce_sequence_number = BooleanValue::Get(produce_sequence_number->second);
	}

	// Include historical identity sources even when they are absent from the selected output schema.
	map<uint64_t, LogicalType> sources;
	for (const auto &spec : metadata.partition_specs) {
		for (const auto &field : spec.second.fields) {
			auto type = IcebergPartitionConstants::GetType(field.source_id, schema, metadata.GetSchemas());
			if (field.transform == IcebergTransformType::IDENTITY && type) {
				sources.emplace(field.source_id, *type);
			}
		}
	}
	child_list_t<LogicalType> constants;
	for (const auto &source : sources) {
		constants.emplace_back(std::to_string(source.first), source.second);
	}
	ret->partition_type = LogicalType::STRUCT(std::move(constants));

	for (auto &column : IcebergScanTaskCodec::Columns(ret->partition_type, IcebergScanTaskCodec::SchemaType(schema))) {
		names.emplace_back(column.first);
		return_types.push_back(column.second);
	}
	return std::move(ret);
}

static void IcebergScanPlanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<IcebergScanPlanBindData>();
	auto &state = data.global_state->Cast<IcebergScanPlanGlobalState>();
	if (state.done) {
		return;
	}
	idx_t count = 0;
	for (; count < STANDARD_VECTOR_SIZE; count++, state.file_id++) {
		auto task = state.planner.GetScanTask(state.file_id);
		if (!task) {
			state.done = true;
			break;
		}
		// Server planning uses synthetic sequence numbers internally; export only when requested.
		if (!bind.produce_sequence_number) {
			task->sequence_number = nullopt;
		}
		IcebergScanTaskCodec::WriteTask(*task, output, count);
	}
	auto snapshot = bind.scan_info->snapshot_info.snapshot;
	IcebergScanTaskCodec::WriteContext(output, count, snapshot ? snapshot->snapshot_id : nullopt,
	                                   bind.scan_info->snapshot_info.schema_id, state.metadata);
}

TableFunctionSet IcebergFunctions::GetIcebergScanPlanFunction() {
	TableFunctionSet function_set("iceberg_scan_plan");
	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergScanPlanFunction, IcebergScanPlanBind,
	                         IcebergScanPlanGlobalState::Init);
	fun.named_parameters["produce_sequence_number"] = LogicalType::BOOLEAN;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP_MS;
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
