
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/equality_delete_file.hpp"
#include "rest_catalog/objects/position_delete_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DeleteFile {
public:
	DeleteFile();
	DeleteFile(const DeleteFile &) = delete;
	DeleteFile &operator=(const DeleteFile &) = delete;
	DeleteFile(DeleteFile &&) = default;
	DeleteFile &operator=(DeleteFile &&) = default;

public:
	// Deserialization
	static DeleteFile FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	DeleteFile Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<PositionDeleteFile> position_delete_file;
	optional<EqualityDeleteFile> equality_delete_file;
};

} // namespace rest_api_objects
} // namespace duckdb
