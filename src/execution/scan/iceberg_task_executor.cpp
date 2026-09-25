#include "execution/scan/iceberg_task_executor.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/iceberg_multi_file_reader.hpp"

namespace duckdb {

IcebergTaskExecutionContext::IcebergTaskExecutionContext(IcebergTableMetadata metadata_p, int32_t schema_id)
    : metadata(std::move(metadata_p)), schema(metadata.GetSchemaFromId(schema_id)) {
}

namespace {

struct IcebergTaskScanInfo : public TableFunctionInfo {
	shared_ptr<IcebergTaskExecutionContext> execution;
	OpenFileInfo file;
	IcebergFileScanTask task;
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
		for (auto &column : info.execution->schema.columns) {
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
		auto deletes = execution.deletes.ProcessDeletes(delete_context, info.file.path, info.task.delete_files);
		return InitializeTaskReader(reader_data, bind, columns, column_ids, filters, context, gstate,
		                            execution.metadata.GetSchemas(), execution.metadata.mappings, std::move(deletes),
		                            info.task.partition_constants);
	}
};

} // namespace

IcebergTaskExecutor::IcebergTaskExecutor(ExecutionContext &context, shared_ptr<IcebergTaskExecutionContext> execution,
                                         IcebergFileScanTask task) {
	auto info = make_shared_ptr<IcebergTaskScanInfo>();
	info->execution = std::move(execution);
	info->file = IcebergMultiFileReader::FileInfo(task.file_path, task.file_format, task.file_size_in_bytes,
	                                              task.first_row_id, task.sequence_number);
	info->task = std::move(task);
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context.client, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan"));
	function = *entry.functions.GetFunctionByArguments(context.client, {LogicalType::VARCHAR});
	function.function_info = info;
	function.get_multi_file_reader = IcebergTaskReader::CreateInstance;
	function.late_materialization = false;
	vector<Value> arguments {Value(info->file.path)};
	named_parameter_map_t parameters;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(arguments, parameters, input_types, input_names, nullptr, nullptr, function, ref);
	vector<LogicalType> types;
	vector<Identifier> names;
	bind = function.bind(context.client, bind_input, types, names);
	vector<column_t> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		ids.push_back(i);
	}
	TableFunctionInitInput init(bind.get(), ids, {}, nullptr);
	global = function.init_global(context.client, init);
	global->Cast<MultiFileGlobalState>().max_threads = 1;
	local = function.init_local(context, init, global.get());
}

IcebergTaskExecutor::~IcebergTaskExecutor() = default;

bool IcebergTaskExecutor::Read(ClientContext &context, DataChunk &output) {
	output.Reset();
	if (finished) {
		return false;
	}
	TableFunctionInput input(bind.get(), local.get(), global.get());
	function.function(context, input, output);
	finished = output.size() == 0;
	return !finished;
}

} // namespace duckdb
