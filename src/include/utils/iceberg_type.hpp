#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

class IcebergTypeRenamer {
public:
	static string GetIcebergTypeString(LogicalType &type);
};

class IcebergTypeHelper {
public:
	static rest_api_objects::Type CreateIcebergRestType(LogicalType &type, idx_t &column_id);
};

} // namespace duckdb
