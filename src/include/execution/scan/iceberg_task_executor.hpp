#pragma once

#include "duckdb/function/table_function.hpp"
#include "iceberg_options.hpp"
#include "planning/deletes/iceberg_delete_file_scanner.hpp"
#include "planning/scan_plan/iceberg_scan_task.hpp"

namespace duckdb {

//! Query-owned metadata and delete caches shared by independently executing tasks.
//! Schema references remain valid for the lifetime of this immovable context.
struct IcebergTaskExecutionContext {
	IcebergTaskExecutionContext(IcebergTableMetadata metadata, int32_t schema_id);
	IcebergTaskExecutionContext(const IcebergTaskExecutionContext &) = delete;
	IcebergTaskExecutionContext &operator=(const IcebergTaskExecutionContext &) = delete;

	const IcebergTableMetadata metadata;
	const IcebergTableSchema &schema;
	const IcebergOptions options;
	IcebergDeleteExecutionState deletes;
};

//! Executes one already-decoded file task. No SQL task layout or planner is needed.
//! Each instance is local to one worker; tasks share only their execution context.
class IcebergTaskExecutor {
public:
	IcebergTaskExecutor(ExecutionContext &context, shared_ptr<IcebergTaskExecutionContext> execution,
	                    IcebergFileScanTask task);
	~IcebergTaskExecutor();

	//! Produces one chunk, or false at exhaustion. Destruction also supports early termination.
	bool Read(ClientContext &context, DataChunk &output);

private:
	//! Reverse destruction order releases local/global scanners before bind data and
	//! the function info that owns the task and shared execution context.
	TableFunction function;
	unique_ptr<FunctionData> bind;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
	bool finished = false;
};

} // namespace duckdb
