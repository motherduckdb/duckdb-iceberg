
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Snapshot {
public:
	Snapshot();
	Snapshot(const Snapshot &) = delete;
	Snapshot &operator=(const Snapshot &) = delete;
	Snapshot(Snapshot &&) = default;
	Snapshot &operator=(Snapshot &&) = default;
	class Object2 {
	public:
		Object2();
		Object2(const Object2 &) = delete;
		Object2 &operator=(const Object2 &) = delete;
		Object2(Object2 &&) = default;
		Object2 &operator=(Object2 &&) = default;

	public:
		// Deserialization
		static Object2 FromJSON(yyjson_val *obj);
		string TryFromJSON(yyjson_val *obj);

		// Copy
		Object2 Copy() const;

		// Serialization
		void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
		yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	public:
		string operation;
		case_insensitive_map_t<string> additional_properties;
	};

public:
	// Deserialization
	static Snapshot FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	Snapshot Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	int64_t snapshot_id;
	int64_t timestamp_ms;
	string manifest_list;
	Object2 summary;
	optional<int64_t> parent_snapshot_id;
	optional<int64_t> sequence_number;
	optional<int64_t> first_row_id;
	optional<int64_t> added_rows;
	optional<int32_t> schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
