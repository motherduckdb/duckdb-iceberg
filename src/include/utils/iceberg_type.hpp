
#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "rest_catalog/objects/type.hpp"
#include "functional"

namespace duckdb {

class IcebergTypeHelper {
public:
	static rest_api_objects::Type CreateIcebergRestType(const LogicalType &type, std::function<idx_t()> get_next_id);
	static string LogicalTypeToIcebergType(const LogicalType &type);
};

} // namespace duckdb
