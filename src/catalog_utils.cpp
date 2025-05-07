#include "catalog_utils.hpp"
#include "iceberg_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_transaction.hpp"

#include <iostream>

namespace duckdb {

string ICUtils::LogicalToIcebergType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::TINYINT:
	case LogicalType::UTINYINT:
		return "tinyint";
	case LogicalType::SMALLINT:
	case LogicalType::USMALLINT:
		return "smallint";
	case LogicalType::INTEGER:
	case LogicalType::UINTEGER:
		return "int";
	case LogicalType::BIGINT:
	case LogicalType::UBIGINT:
		return "long";
	case LogicalType::VARCHAR:
		return "string";
	case LogicalType::DOUBLE:
		return "double";
	case LogicalType::FLOAT:
		return "float";
	case LogicalType::BOOLEAN:
		return "boolean";
	case LogicalType::TIMESTAMP:
		return "timestamp";
	case LogicalType::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalType::BLOB:
		return "binary";
	case LogicalType::DATE:
		return "date";
	case LogicalTypeId::DECIMAL: {
		uint8_t precision = DecimalType::GetWidth(input);
		uint8_t scale = DecimalType::GetScale(input);
		return "decimal(" + std::to_string(precision) + ", " + std::to_string(scale) + ")";
	}
	// case LogicalTypeId::ARRAY:
	// case LogicalTypeId::STRUCT:
	// case LogicalTypeId::MAP:
	default:
		break;
	}

	throw InvalidInputException("Unsupported type: %s", input.ToString());
}

string ICUtils::TypeToString(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::VARCHAR:
		return "TEXT";
	case LogicalType::UTINYINT:
		return "TINYINT UNSIGNED";
	case LogicalType::USMALLINT:
		return "SMALLINT UNSIGNED";
	case LogicalType::UINTEGER:
		return "INTEGER UNSIGNED";
	case LogicalType::UBIGINT:
		return "BIGINT UNSIGNED";
	case LogicalType::TIMESTAMP:
		return "DATETIME";
	case LogicalType::TIMESTAMP_TZ:
		return "TIMESTAMP";
	default:
		return input.ToString();
	}
}

LogicalType ICUtils::ToICType(const LogicalType &input) {
	// todo do we need this mapping?
	throw NotImplementedException("ToICType not yet implemented");
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		throw NotImplementedException("Iceberg does not support arrays - unsupported type \"%s\"", input.ToString());
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		throw NotImplementedException("Iceberg does not support composite types - unsupported type \"%s\"",
		                              input.ToString());
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

yyjson_doc *ICUtils::api_result_to_doc(const string &api_result) {
	auto *doc = yyjson_read(api_result.c_str(), api_result.size(), 0);
	auto *root = yyjson_doc_get_root(doc);
	auto *error = yyjson_obj_get(root, "error");
	if (error != NULL) {
		try {
			string err_msg = IcebergUtils::TryGetStrFromObject(error, "message");
			throw InvalidInputException(err_msg);
		} catch (InvalidConfigurationException &e) {
			// keep going, we will throw the whole api result as an error message
			throw InvalidConfigurationException(api_result);
		}
		throw InvalidConfigurationException("Could not parse api_result");
	}
	return doc;
}

} // namespace duckdb
