#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class IcebergTypeRenamer {
public:
	static string GetIcebergTypeString(LogicalType &type);
};

} // namespace duckdb
