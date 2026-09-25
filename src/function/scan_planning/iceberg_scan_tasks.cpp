#include "function/iceberg_functions.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "function/scan_planning/iceberg_scan_task_codec.hpp"
#include "execution/scan/iceberg_task_executor.hpp"

namespace duckdb {

using TaskCodec = IcebergScanTaskCodec;

struct IcebergScanTasksBindData : public TableFunctionData {
	TaskCodec::InputLayout layout;
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
		auto metadata = TaskCodec::ReadMetadata(text, IntegerValue::Get(schema), snapshot, bind.layout.schema_type);
		auto result = make_shared_ptr<IcebergTaskExecutionContext>(std::move(metadata), IntegerValue::Get(schema));

		metadata_json = text;
		schema_id = schema;
		snapshot_id = snapshot;
		execution = result;
		return result;
	}
};

struct IcebergScanTasksLocalState : public LocalTableFunctionState {
	idx_t row = 0;
	Vector metadata_json {LogicalType::JSON()};
	unique_ptr<IcebergTaskExecutor> active;
};

static unique_ptr<FunctionData> IcebergScanTasksBind(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &types, vector<Identifier> &names) {
	auto result = make_uniq<IcebergScanTasksBindData>();
	result->layout = TaskCodec::BindInput(input, types, names);
	return std::move(result);
}

static unique_ptr<IcebergTaskExecutor> StartTask(ExecutionContext &context, const IcebergScanTasksBindData &bind,
                                                 IcebergScanTasksGlobalState &global, IcebergScanTasksLocalState &local,
                                                 DataChunk &input) {
	if (input.GetValue(bind.layout.columns[TaskCodec::METADATA], local.row).IsNull()) {
		throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
	}
	auto execution =
	    global.GetContext(bind, local.metadata_json.GetValue(local.row),
	                      TaskCodec::ReadValue(input, bind.layout, local.row, TaskCodec::SCHEMA_ID),
	                      TaskCodec::ReadValue(input, bind.layout, local.row, TaskCodec::SNAPSHOT_ID, true));
	auto task = TaskCodec::ReadTask(input, bind.layout, local.row, execution->metadata, execution->schema);
	return make_uniq<IcebergTaskExecutor>(context, std::move(execution), std::move(task));
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
		if (local.active->Read(context.client, output)) {
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
