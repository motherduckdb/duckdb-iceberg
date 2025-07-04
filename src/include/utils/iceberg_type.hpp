
#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

class IcebergTypeHelper {
public:
	static rest_api_objects::Type CreateIcebergRestType(LogicalType &type, idx_t &column_id);
	static string GetIcebergTypeString(LogicalType &type);
};

} // namespace duckdb
