#include "function/iceberg_functions.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "core/expression/iceberg_value.hpp"
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
	vector<uint64_t> partition_source_ids;
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

	// A stable union of identity sources across specs, using the selected schema's types.
	map<uint64_t, LogicalType> sources;
	for (const auto &spec : metadata.partition_specs) {
		for (const auto &field : spec.second.fields) {
			auto column = schema.TryGetColumnByFieldId(field.source_id);
			if (field.transform == IcebergTransformType::IDENTITY && column) {
				sources.emplace(field.source_id, column->type);
			}
		}
	}
	child_list_t<LogicalType> constants;
	for (const auto &source : sources) {
		ret->partition_source_ids.push_back(source.first);
		constants.emplace_back(std::to_string(source.first), source.second);
	}
	ret->partition_type = LogicalType::STRUCT(std::move(constants));

	for (auto &column : IcebergScanTaskCodec::Columns(ret->partition_type, IcebergScanTaskCodec::SchemaType(schema))) {
		names.emplace_back(column.first);
		return_types.push_back(column.second);
	}
	return std::move(ret);
}

static unordered_map<int32_t, Value> PartitionConstants(const IcebergScanPlanBindData &bind,
                                                        const IcebergScanTask &task, int32_t partition_spec_id) {
	auto &metadata = bind.scan_info->metadata;
	auto spec = metadata.partition_specs.find(partition_spec_id);
	if (spec == metadata.partition_specs.end()) {
		throw InvalidConfigurationException("'partition_spec_id' %d doesn't exist in the metadata", partition_spec_id);
	}
	// Match the reader: the last spec field for a source determines its fallback.
	unordered_map<uint64_t, idx_t> field_indexes;
	for (idx_t i = 0; i < spec->second.fields.size(); i++) {
		field_indexes[spec->second.fields[i].source_id] = i;
	}
	unordered_map<int32_t, Value> values;
	auto &children = StructType::GetChildTypes(bind.partition_type);
	for (idx_t i = 0; i < bind.partition_source_ids.size(); i++) {
		auto &type = children[i].second;
		Value value(type);
		auto index = field_indexes.find(bind.partition_source_ids[i]);
		do {
			if (index == field_indexes.end()) {
				break;
			}
			auto &field = spec->second.fields[index->second];
			if (field.transform != IcebergTransformType::IDENTITY) {
				break;
			}
			for (const auto &partition : task.manifest_entry.entry.data_file.partition_info) {
				if (partition.field_id != field.partition_field_id || partition.value.IsNull()) {
					continue;
				}
				value = IcebergValue::TransformPartitionValue(partition.value, type);
				break;
			}
		} while (false);
		values.emplace(bind.partition_source_ids[i], std::move(value));
	}
	return values;
}

static vector<IcebergDeleteFile> DeleteFiles(const IcebergScanPlanner &planner, const IcebergScanTask &task) {
	vector<IcebergDeleteFile> files;
	for (auto ref : task.delete_files) {
		auto &manifest = planner.GetDeleteManifest(ref);
		files.emplace_back(manifest.GetManifestEntries()[ref.entry_idx].data_file);
	}
	return files;
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
		auto &file = task->manifest_entry.entry.data_file;
		IcebergFileScanTask row;
		row.file_path = task->file_path;
		row.file_format = file.file_format;
		row.file_size_in_bytes = file.file_size_in_bytes;
		row.record_count = file.record_count;
		int32_t partition_spec_id;
		optional<int64_t> sequence_number;
		state.planner.WithManifestFile(task->manifest_entry, IcebergManifestContentType::DATA,
		                               [&](const IcebergManifestFile &manifest) {
			                               partition_spec_id = manifest.partition_spec_id;
			                               if (bind.produce_sequence_number) {
				                               sequence_number = task->manifest_entry.entry.GetSequenceNumber(manifest);
			                               }
		                               });
		// Do not expose the synthetic sequence numbers used internally by server planning.
		row.sequence_number = sequence_number;
		row.first_row_id =
		    task->manifest_entry.HasFirstRowId() ? optional<int64_t>(task->manifest_entry.GetFirstRowId()) : nullopt;
		row.partition_spec_id = partition_spec_id;
		row.partition_constants = PartitionConstants(bind, *task, partition_spec_id);
		row.delete_files = DeleteFiles(state.planner, *task);
		IcebergScanTaskCodec::WriteTask(row, output, count);
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
