//===----------------------------------------------------------------------===//
//                         DuckDB
//
// utils/json_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/file_system.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

class JSONUtils {
public:
	static string JsonDocToString(yyjson_mut_doc *doc);
	static string json_to_string(yyjson_mut_doc *doc, yyjson_write_flag flags = YYJSON_WRITE_PRETTY);
};

} // namespace duckdb
