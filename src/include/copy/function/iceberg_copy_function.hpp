#pragma once

#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct IcebergCopyFunction {
	static CopyFunction Create();
};

} // namespace duckdb
