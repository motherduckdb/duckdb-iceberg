#include "function/iceberg_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "function/scan_planning/iceberg_scan_task_codec.hpp"

namespace duckdb {

using TaskCodec = IcebergScanTaskCodec;

struct IcebergScanTasksBindData : public TableFunctionData {
	TaskCodec::InputLayout layout;
};

struct IcebergTaskExecutionContext {
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas()};
	optional_ptr<const IcebergTableSchema> schema;
	IcebergOptions options;
	IcebergDeleteExecutionState deletes;
};

struct IcebergScanTasksGlobalState : public GlobalTableFunctionState {
	mutex lock;
	string metadata_json;
	Value schema_id;
	Value snapshot_id;
	shared_ptr<IcebergTaskExecutionContext> execution;

	idx_t MaxThreads() const override {
		return MAX_THREADS;
	}

	shared_ptr<IcebergTaskExecutionContext> GetContext(const IcebergScanTasksBindData &bind, const Value &json,
	                                                   const Value &schema, const Value &snapshot) {
		if (json.IsNull() || schema.IsNull()) {
			throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
		}
		lock_guard<mutex> guard(lock);
		auto &text = StringValue::Get(json);
		if (execution) {
			if (text != metadata_json || !Value::NotDistinctFrom(schema, schema_id) ||
			    !Value::NotDistinctFrom(snapshot, snapshot_id)) {
				throw InvalidInputException(
				    "iceberg_scan_tasks requires one metadata document, schema ID, and snapshot ID");
			}
			return execution;
		}
		auto result = make_shared_ptr<IcebergTaskExecutionContext>();
		result->metadata = TaskCodec::ReadMetadata(text, IntegerValue::Get(schema), snapshot, bind.layout.schema_type);
		result->schema = result->metadata.GetSchemaFromId(IntegerValue::Get(schema));

		metadata_json = text;
		schema_id = schema;
		snapshot_id = snapshot;
		execution = result;
		return result;
	}
};

struct IcebergTaskScanInfo : public TableFunctionInfo {
	shared_ptr<IcebergTaskExecutionContext> execution;
	OpenFileInfo file;
	unordered_map<int32_t, Value> partition_constants;
	vector<IcebergDeleteFile> delete_files;
};

//! The ordinary reader owns mapping and chunk finalization. Only its planner-facing
//! binding and task lookup are replaced for a materialized single-file task.
struct IcebergTaskReader : public IcebergMultiFileReader {
	explicit IcebergTaskReader(shared_ptr<TableFunctionInfo> info) : IcebergMultiFileReader(std::move(info)) {
	}

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &function) {
		return make_uniq<IcebergTaskReader>(function.function_info);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, const FileGlobInput &) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		return make_shared_ptr<SimpleMultiFileList>(vector<OpenFileInfo> {info.file});
	}

	bool Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &types, vector<Identifier> &names,
	          MultiFileReaderBindData &bind) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		for (auto &column : info.execution->schema->columns) {
			types.push_back(column->type);
			names.emplace_back(column->name);
			bind.schema.push_back(column->GetMultiFileColumnDefinition());
		}
		QueryResult::DeduplicateColumns(names);
		for (idx_t i = 0; i < names.size(); i++) {
			bind.schema[i].name = names[i];
		}
		bind.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
		return true;
	}

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind,
	                                      const vector<MultiFileColumnDefinition> &columns,
	                                      const vector<ColumnIndex> &column_ids, optional_ptr<TableFilterSet> filters,
	                                      ClientContext &context, MultiFileGlobalState &gstate) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		auto &execution = *info.execution;
		IcebergDeleteExecutionContext delete_context {context, FileSystem::GetFileSystem(context),
		                                              execution.metadata.location, execution.options,
		                                              execution.metadata};
		auto deletes = execution.deletes.ProcessDeletes(delete_context, info.file.path, info.delete_files);
		return InitializeTaskReader(reader_data, bind, columns, column_ids, filters, context, gstate,
		                            execution.metadata.GetSchemas(), execution.metadata.mappings, std::move(deletes),
		                            info.partition_constants);
	}
};

//! Member order keeps bind data, descriptors and execution context alive until
//! both scanner states have been destroyed.
struct IcebergActiveTask {
	TableFunction function;
	unique_ptr<FunctionData> bind;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
};

struct IcebergScanTasksLocalState : public LocalTableFunctionState {
	idx_t row = 0;
	Vector metadata_json {LogicalType::JSON()};
	unique_ptr<IcebergActiveTask> active;
};

static unique_ptr<FunctionData> IcebergScanTasksBind(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &types, vector<Identifier> &names) {
	auto result = make_uniq<IcebergScanTasksBindData>();
	result->layout = TaskCodec::BindInput(input, types, names);
	return std::move(result);
}

static unique_ptr<IcebergActiveTask> StartTask(ExecutionContext &context, const IcebergScanTasksBindData &bind,
                                               IcebergScanTasksGlobalState &global, IcebergScanTasksLocalState &local,
                                               DataChunk &input) {
	if (input.GetValue(bind.layout.columns[TaskCodec::METADATA], local.row).IsNull()) {
		throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
	}
	auto info = make_shared_ptr<IcebergTaskScanInfo>();
	info->execution =
	    global.GetContext(bind, local.metadata_json.GetValue(local.row),
	                      TaskCodec::ReadValue(input, bind.layout, local.row, TaskCodec::SCHEMA_ID),
	                      TaskCodec::ReadValue(input, bind.layout, local.row, TaskCodec::SNAPSHOT_ID, true));
	auto task = TaskCodec::ReadTask(input, bind.layout, local.row, info->execution->metadata, *info->execution->schema);
	info->file = IcebergMultiFileReader::FileInfo(task.file_path, task.file_format, task.file_size_in_bytes,
	                                              task.first_row_id, task.sequence_number);
	info->partition_constants = std::move(task.partition_constants);
	info->delete_files = std::move(task.delete_files);

	auto result = make_uniq<IcebergActiveTask>();
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context.client, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan"));
	result->function = *entry.functions.GetFunctionByArguments(context.client, {LogicalType::VARCHAR});
	result->function.function_info = info;
	result->function.get_multi_file_reader = IcebergTaskReader::CreateInstance;
	result->function.late_materialization = false;
	vector<Value> arguments {Value(info->file.path)};
	named_parameter_map_t parameters;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(arguments, parameters, input_types, input_names, nullptr, nullptr,
	                                  result->function, ref);
	vector<LogicalType> types;
	vector<Identifier> names;
	result->bind = result->function.bind(context.client, bind_input, types, names);
	vector<column_t> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		ids.push_back(i);
	}
	TableFunctionInitInput init(result->bind.get(), ids, {}, nullptr);
	result->global = result->function.init_global(context.client, init);
	result->global->Cast<MultiFileGlobalState>().max_threads = 1;
	result->local = result->function.init_local(context, init, result->global.get());
	return result;
}

static OperatorResultType IcebergScanTasksFunction(ExecutionContext &context, TableFunctionInput &data,
                                                   DataChunk &input, DataChunk &output) {
	auto &bind = data.bind_data->Cast<IcebergScanTasksBindData>();
	auto &global = data.global_state->Cast<IcebergScanTasksGlobalState>();
	auto &local = data.local_state->Cast<IcebergScanTasksLocalState>();
	if (local.row == 0 && !local.active) {
		// A previous constant input leaves the cast result constant; start with writable flat storage.
		local.metadata_json.Initialize();
		VectorOperations::Cast(context.client, input.data[bind.layout.columns[TaskCodec::METADATA]],
		                       local.metadata_json, input.size());
	}
	while (local.row < input.size()) {
		context.client.InterruptCheck();
		if (!local.active) {
			local.active = StartTask(context, bind, global, local, input);
		}
		auto &task = *local.active;
		TableFunctionInput scan_input(task.bind.get(), task.local.get(), task.global.get());
		output.Reset();
		task.function.function(context.client, scan_input, output);
		if (output.size()) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		local.active.reset();
		local.row++;
	}
	local.row = 0;
	return OperatorResultType::NEED_MORE_INPUT;
}

TableFunctionSet IcebergFunctions::GetIcebergScanTasksFunction() {
	TableFunction function("iceberg_scan_tasks", {LogicalType::TABLE}, nullptr, IcebergScanTasksBind);
	function.in_out_function = IcebergScanTasksFunction;
	function.init_global = [](ClientContext &, TableFunctionInitInput &) -> unique_ptr<GlobalTableFunctionState> {
		return make_uniq<IcebergScanTasksGlobalState>();
	};
	function.init_local = [](ExecutionContext &, TableFunctionInitInput &,
	                         GlobalTableFunctionState *) -> unique_ptr<LocalTableFunctionState> {
		return make_uniq<IcebergScanTasksLocalState>();
	};
	function.projection_pushdown = false;
	function.filter_pushdown = false;
	return TableFunctionSet(function);
}

} // namespace duckdb
