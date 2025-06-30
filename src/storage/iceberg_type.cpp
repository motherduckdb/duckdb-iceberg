#include "duckdb/common/string_util.hpp"
#include "storage/iceberg_type.hpp"

namespace duckdb {

string IcebergTypeRenamer::GetIcebergTypeString(LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL:
		return "decimal";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::STRUCT:
	default:
		throw NotImplementedException("Type not supported in Duckdb-Iceberg");
	}
}

} // namespace duckdb
