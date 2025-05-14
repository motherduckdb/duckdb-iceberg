#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_util.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_multi_file_reader.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>
#include <numeric>

namespace duckdb {

static void AddNamedParameters(TableFunction &fun) {
	fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
}

TableFunctionSet IcebergFunctions::GetIcebergScanFunction(DatabaseInstance &instance) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read

	TableFunctionSet function_set("iceberg_scan");

	auto &parquet_scan = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergMultiFileReader::CreateInstance;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = nullptr;
		function.deserialize = nullptr;
		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = nullptr;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		AddNamedParameters(function);

		function.name = "iceberg_scan";

		function_set.AddFunction(function);
	}

	D_ASSERT(function_set.functions.empty() == false);
	auto model_function = function_set.functions[0];

	// todo: may not work, need checking; may need to fiddle with parsed arguments/named params
	// todo: do we need this at all?
	//  old function overloads
	auto tf1 = model_function;
	tf1.arguments = {LogicalType::VARCHAR, LogicalType::UBIGINT};
	tf1.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	tf1.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	tf1.named_parameters["mode"] = LogicalType::VARCHAR;
	tf1.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	tf1.named_parameters["version"] = LogicalType::VARCHAR;
	tf1.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(tf1);

	auto tf2 = model_function;
	tf2.arguments = {LogicalType::VARCHAR, LogicalType::TIMESTAMP};
	tf2.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	tf2.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	tf2.named_parameters["mode"] = LogicalType::VARCHAR;
	tf2.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	tf2.named_parameters["version"] = LogicalType::VARCHAR;
	tf2.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(tf2);

	return function_set;
}

} // namespace duckdb
