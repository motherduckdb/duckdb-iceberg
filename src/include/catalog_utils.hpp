
#pragma once

#include "duckdb.hpp"
#include "catalog_api.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;
namespace duckdb {
class IRCSchemaEntry;
class IRCTransaction;

enum class ICTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

struct ICType {
	idx_t oid = 0;
	ICTypeAnnotation info = ICTypeAnnotation::STANDARD;
	vector<ICType> children;
};

class ICUtils {
public:
	static LogicalType ToICType(const LogicalType &input);
	static string TypeToString(const LogicalType &input);
	static string LogicalToIcebergType(const LogicalType &input);
	static yyjson_doc *api_result_to_doc(const string &api_result);
};

struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) {
		yyjson_doc_free(doc);
	}
	void operator()(yyjson_mut_doc *doc) {
		yyjson_mut_doc_free(doc);
	}
};

} // namespace duckdb
