#pragma once

#include "catalog/rest/iceberg_request_task.hpp"

#include <chrono>

namespace duckdb {

//! Schedules owned requests on one producer and drains them when this scope ends.
//! The caller must keep the context and catalog alive; publication remains the caller's responsibility.
class IcebergRequestExecutor {
public:
	IcebergRequestExecutor(ClientContext &context, IcebergCatalog &catalog)
	    : context(context), catalog(catalog), executor(context, TaskSchedulerType::ASYNC) {
	}

	template <class REQUEST>
	shared_ptr<IcebergRequestResult<typename REQUEST::Result>> Schedule(REQUEST request) {
		auto result = make_shared_ptr<IcebergRequestResult<typename REQUEST::Result>>();
		executor.ScheduleTask(
		    make_uniq<IcebergRequestTask<REQUEST>>(executor, context, catalog, std::move(request), result));
		return result;
	}

	//! Consume a result scheduled by this executor, helping only its producer's tasks while waiting.
	//! Other requests and task cleanup may still be running when this returns.
	template <class RESULT>
	RESULT WaitAndTakeResult(IcebergRequestResult<RESULT> &result) {
		while (true) {
			context.InterruptCheck();
			if (executor.HasError()) {
				executor.ThrowError();
			}
			if (result.IsReady()) {
				return result.TakeResult();
			}
			shared_ptr<Task> task;
			if (executor.GetTask(task)) {
				const auto task_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
				D_ASSERT(task_result != TaskExecutionResult::TASK_NOT_FINISHED);
			} else {
				unique_lock<mutex> guard(result.lock);
				// Interruption does not notify this condition variable, so periodically check it on the caller.
				result.completion.wait_for(guard, std::chrono::milliseconds(50), [&]() { return result.ready; });
			}
		}
	}

	//! Executor failures abort the operation; individual request failures are handled by the caller.
	bool HasError() {
		return executor.HasError();
	}

	//! Join at the operation boundary and surface executor failures. Destruction cancels and drains on unwind.
	void Drain() {
		executor.WorkOnTasks();
		context.InterruptCheck();
	}

private:
	ClientContext &context;
	IcebergCatalog &catalog;
	TaskExecutor executor;
};

} // namespace duckdb
