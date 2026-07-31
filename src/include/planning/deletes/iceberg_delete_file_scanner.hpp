#pragma once

#include "planning/deletes/iceberg_delete_planner.hpp"

namespace duckdb {

struct IcebergDeleteFileScanner {
	static void ScanFiles(const IcebergDeletePlanningContext &context);
};

} // namespace duckdb
