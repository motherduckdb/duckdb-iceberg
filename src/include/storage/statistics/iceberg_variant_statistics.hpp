//===----------------------------------------------------------------------===//
//                         DuckDB - Iceberg
//
// storage/statistics/iceberg_variant_statistics.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

//! Accumulates statistics for the shredded sub-fields of a single VARIANT column.
//!
//! The parquet writer emits one stats entry per leaf of a (shredded) variant column:
//! a "metadata" leaf (which carries the variant shredding type), an untyped "value"
//! leaf, and a "typed_value" leaf for every shredded field. Because these arrive as
//! separate entries (and in no guaranteed order), we buffer them here and only resolve
//! the per-field types and serialize the bounds in Finalize().
//!
//! Finalize() builds two Iceberg "bounds variants" - one for the lower bounds and one
//! for the upper bounds - whose object keys are normalized JSON paths (e.g.
//! "$['person']['age']") and whose values are the typed primitive bounds. Each is
//! serialized to the parquet-variant encoding (metadata + value concatenated), which is
//! exactly what the Iceberg spec stores in the manifest lower/upper bounds for a variant.
//!
//! First-pass limitations: bounds are emitted for any shredded field that has a min/max,
//! without checking that the field is uniformly typed (fully shredded). Fields whose type
//! cannot be resolved or whose bound cannot be cast are silently skipped.
class IcebergVariantBounds {
public:
	//! Ingest one stats entry whose path descends into the variant.
	//! full_path is the parsed (unquoted) column path, e.g. {"v","typed_value","age","typed_value"}.
	//! variant_field_start is the index of the first path element inside the variant (the name_offset
	//! returned by IcebergTableSchema::GetFromPath). col_stats is the list of (name, value) stat structs.
	void AddStatsEntry(const vector<string> &full_path, idx_t variant_field_start, const vector<Value> &col_stats);

	//! True if a shredding type and at least one shredded field bound were collected.
	bool HasBounds() const;

	//! Build the serialized parquet-variant bounds blobs (metadata + value concatenated) for the
	//! lower and upper bounds. Returns false (leaving outputs untouched) if nothing should be written.
	//! has_lower / has_upper indicate which blobs were produced.
	bool Finalize(ClientContext &context, bool &has_lower, string &lower_blob, bool &has_upper, string &upper_blob);

private:
	struct FieldBound {
		vector<string> field_names; // empty == variant root ("$")
		bool has_min = false;
		string min_value;
		bool has_max = false;
		string max_value;
	};

	bool has_variant_type = false;
	string variant_type_str;
	vector<FieldBound> fields;
};

} // namespace duckdb
