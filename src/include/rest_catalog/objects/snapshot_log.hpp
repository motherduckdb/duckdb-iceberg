
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotLog {
public:
	SnapshotLog();
	SnapshotLog(const SnapshotLog &) = delete;
	SnapshotLog &operator=(const SnapshotLog &) = delete;
	SnapshotLog(SnapshotLog &&) = default;
	SnapshotLog &operator=(SnapshotLog &&) = default;
	class Object3 {
	public:
		Object3();
		Object3(const Object3 &) = delete;
		Object3 &operator=(const Object3 &) = delete;
		Object3(Object3 &&) = default;
		Object3 &operator=(Object3 &&) = default;

	public:
		// Deserialization
		static Object3 FromJSON(yyjson_val *obj);
		string TryFromJSON(yyjson_val *obj);

		// Copy
		Object3 Copy() const;

		// Serialization
		void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
		yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

	public:
		int64_t snapshot_id;
		int64_t timestamp_ms;
	};

public:
	// Deserialization
	static SnapshotLog FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	SnapshotLog Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<Object3> value;
};

} // namespace rest_api_objects
} // namespace duckdb
