
#include "rest_catalog/objects/partition_statistics_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PartitionStatisticsFile PartitionStatisticsFile::FromJSON(yyjson_val *obj) {
	PartitionStatisticsFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PartitionStatisticsFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "PartitionStatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
	if (!statistics_path_val) {
		return "PartitionStatisticsFile required property 'statistics-path' is missing";
	} else {
		if (yyjson_is_str(statistics_path_val)) {
			statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'statistics_path' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(statistics_path_val));
		}
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "PartitionStatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (yyjson_is_sint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else if (yyjson_is_uint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_uint(file_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "PartitionStatisticsFile property 'file_size_in_bytes' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(file_size_in_bytes_val));
		}
	}
	return "";
}

yyjson_mut_val *PartitionStatisticsFile::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: statistics-path
	yyjson_mut_obj_add_str(doc, obj, "statistics-path", statistics_path.c_str());

	// Serialize: file-size-in-bytes
	yyjson_mut_obj_add_sint(doc, obj, "file-size-in-bytes", file_size_in_bytes);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
