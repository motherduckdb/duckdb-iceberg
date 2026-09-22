#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

class IcebergCatalog;

//! One task writes this result. The caller may consume it only after draining the executor.
//! The result holder, execution context and catalog must all outlive the executor's tasks.
template <class RESULT>
class IcebergRequestResult {
public:
	RESULT TakeResult() {
		if (error.HasError()) {
			error.Throw();
		}
		if (!result) {
			throw InternalException("Iceberg request result is not available or was already consumed");
		}
		auto value = std::move(*result);
		result.reset();
		return value;
	}

private:
	template <class REQUEST>
	friend class IcebergRequestTask;

	//! The outer optional records completion, even when RESULT is itself an empty optional (a refused listing).
	optional<RESULT> result;
	ErrorData error;
};

//! Executes an owned request without retaining catalog entries or publishing its response.
//! Scheduling, draining, and handling request failures remain the caller's responsibility.
template <class REQUEST>
class IcebergRequestTask : public BaseExecutorTask {
public:
	using Result = typename REQUEST::Result;

	IcebergRequestTask(TaskExecutor &executor, ClientContext &context, IcebergCatalog &catalog, REQUEST request,
	                   IcebergRequestResult<Result> &result)
	    : BaseExecutorTask(executor), context(context), catalog(catalog), request(std::move(request)), result(result) {
	}

	void ExecuteTask() override {
		if (context.IsInterrupted()) {
			result.error = ErrorData(InterruptException());
			result.error.Throw();
		}
		try {
			result.result.emplace(request.Execute(context, catalog));
		} catch (std::exception &ex) {
			// Let the caller decide whether a request failure aborts the operation or is only a warning.
			result.error = ErrorData(ex);
		}
	}

	void Cancel() override {
		result.error = ErrorData(ExceptionType::INTERRUPT, "Iceberg catalog request was cancelled");
	}

	string TaskType() const override {
		return "IcebergRequestTask";
	}

private:
	ClientContext &context;
	IcebergCatalog &catalog;
	REQUEST request;
	IcebergRequestResult<Result> &result;
};

} // namespace duckdb
