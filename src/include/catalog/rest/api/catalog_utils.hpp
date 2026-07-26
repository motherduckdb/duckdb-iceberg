
#pragma once

#include "duckdb.hpp"
#include "duckdb/common/json_document.hpp"

#include "catalog/rest/api/catalog_api.hpp"

namespace duckdb {
class IcebergSchemaEntry;
class IcebergTransaction;

class ICUtils {
public:
	static JSONValue GetErrorMessage(const string &api_result, unique_ptr<JSONDocument> &out_doc);
	static unique_ptr<JSONDocument> APIResultToDoc(const string &api_result);
};

} // namespace duckdb
