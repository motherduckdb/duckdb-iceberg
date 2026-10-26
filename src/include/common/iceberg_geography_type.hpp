#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
class LogicalOperator;

//! DuckDB has no geography type. A geography field is kept in the schema as a placeholder BLOB that remembers
//! its Iceberg type, so the rest of the table stays usable and the field can't be read or written.
struct IcebergGeographyType {
	static LogicalType Get(const string &iceberg_type);
	static bool IsGeography(const LogicalType &type);
	static bool ContainsGeography(const LogicalType &type);
	//! The Iceberg type string, e.g. `geography(OGC:CRS84, spherical)`
	static string GetIcebergType(const LogicalType &type);
	static void Register(ExtensionLoader &loader);
	//! Throws if a bound CREATE TABLE / CREATE TYPE / ALTER TABLE plan declares a geography type, in any catalog:
	//! DuckDB can't write geography values. Called from the Iceberg planner's post-bind hook.
	static void RejectDeclared(ClientContext &context, LogicalOperator &op);
};

} // namespace duckdb
