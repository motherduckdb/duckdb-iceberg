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
//! First-pass limitations: bounds are emitted for fully shredded fields that has a min/max,
//! For any "leaf" stats that are  not typed values, we can infer fully shreddedness from that.
//! If the field is not fully shredded, we do not write the stats.
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
	// path of partially shredded variant values.
	// we do not record stats for partially shredded values since the types may be different
	vector<vector<string>> partial_paths;
};

//! Decodes the parquet-variant bounds blobs produced by IcebergVariantBounds back into a VARIANT Value,
//! so a manifest's lower/upper bound for a shredded VARIANT column can be used for file pruning.
//!
//! The blob is the canonical binary Variant encoding (metadata + value concatenated) of an object whose keys
//! are JSON paths ("$['age']", "$['person']['age']"). The keys are preserved as-is; a pruning consumer rebuilds
//! the matching JSON path (see IcebergVariantBounds' BuildJsonPath convention) to look up a field's bounds.
struct IcebergVariantBoundsReader {
	//! Decode a bounds blob into a VARIANT Value by delegating to the parquet extension's
	//! 'variant_bytes_to_variant' scalar function. Returns false (leaving result untouched) if the blob can't
	//! be decoded (e.g. the parquet extension isn't loaded, or the bytes are malformed).
	static bool Deserialize(ClientContext &context, const string_t &blob, Value &result);

	//! Re-key a decoded bounds VARIANT (whose object keys are JSON paths like "$['age']") to the plain field
	//! names used at the call site ("age", "person.age"), returning a VARIANT Value with the renamed keys. This
	//! lets a filter on the variant column (e.g. "variant_col.age > 60") be evaluated directly against the bound
	//! via IcebergPredicate::MatchBounds. Returns false (leaving result untouched) if the value isn't a VARIANT
	//! object.
	static bool RekeyBoundsVariant(const Value &bounds_variant, Value &result);
};

} // namespace duckdb
