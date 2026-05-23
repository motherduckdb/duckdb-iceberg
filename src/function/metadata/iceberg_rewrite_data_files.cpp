#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/table_identifier.hpp"
#include "maintenance/table_lock_registry.hpp"

namespace duckdb {

namespace {

struct RewriteDataFilesBindData : public TableFunctionData {
	MaintenanceTableKey table_key;
	string strategy = "binpack";
	int64_t target_file_size_bytes = 134217728; // 128 MiB
	int64_t min_input_files = 5;
	bool rewrite_all = false;
	bool emitted = false;
};

static unique_ptr<FunctionData> RewriteDataFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<RewriteDataFilesBindData>();
	bind_data->table_key =
	    ParseMaintenanceTableIdentifier("iceberg_rewrite_data_files", StringValue::Get(input.inputs[0]));

	for (auto &kv : input.named_parameters) {
		auto opt = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if (opt == "strategy") {
			auto strategy = StringValue::Get(val);
			if (strategy != "binpack") {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: only 'binpack' strategy is supported, got '%s'", strategy);
			}
			bind_data->strategy = std::move(strategy);
		} else if (opt == "target_file_size_bytes") {
			auto v = val.GetValue<int64_t>();
			if (v <= 0) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'target_file_size_bytes' must be > 0, got %lld", v);
			}
			bind_data->target_file_size_bytes = v;
		} else if (opt == "min_input_files") {
			auto v = val.GetValue<int64_t>();
			if (v < 1) {
				throw InvalidInputException(
				    "iceberg_rewrite_data_files: 'min_input_files' must be >= 1, got %lld", v);
			}
			bind_data->min_input_files = v;
		} else if (opt == "rewrite_all") {
			bind_data->rewrite_all = BooleanValue::Get(val);
		}
	}

	names.emplace_back("rewritten_data_files");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("added_data_files");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("rewritten_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(bind_data);
}

static void RewriteDataFilesExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<RewriteDataFilesBindData>();
	if (bind_data.emitted) {
		output.SetCardinality(0);
		return;
	}
	bind_data.emitted = true;

	auto guard = TableLockRegistry::GetInstance().TryAcquire(bind_data.table_key, "rewrite_data_files");
	if (!guard.Owns()) {
		throw CatalogException("iceberg_rewrite_data_files: table '%s' is already being compacted by another "
		                        "maintenance action",
		                        bind_data.table_key.table);
	}

	throw NotImplementedException("iceberg_rewrite_data_files: execute not yet wired");
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRewriteDataFilesFunction() {
	TableFunctionSet function_set("iceberg_rewrite_data_files");
	TableFunction tf({LogicalType::VARCHAR}, RewriteDataFilesExecute, RewriteDataFilesBind);
	tf.named_parameters["strategy"] = LogicalType::VARCHAR;
	tf.named_parameters["target_file_size_bytes"] = LogicalType::BIGINT;
	tf.named_parameters["min_input_files"] = LogicalType::BIGINT;
	tf.named_parameters["rewrite_all"] = LogicalType::BOOLEAN;
	function_set.AddFunction(tf);
	return function_set;
}

} // namespace duckdb
