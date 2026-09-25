#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include <functional>

namespace duckdb {

//! Resolves identity-partition fallback values independently of planning or reading.
struct IcebergPartitionConstants {
	using TypeLookup = std::function<optional_ptr<const LogicalType>(int32_t field_id)>;

	//! The last spec field for a source wins, including non-identity transforms.
	//! Missing, NULL, and untyped values are omitted. SQL encoding supplies typed
	//! NULLs where required; readers preserve their normal missing-column defaults.
	static unordered_map<int32_t, Value> Resolve(int32_t spec_id,
	                                             const unordered_map<int32_t, IcebergPartitionSpec> &specs,
	                                             const vector<IcebergPartitionInfo> &partition_values,
	                                             const TypeLookup &lookup_type);
};

} // namespace duckdb
