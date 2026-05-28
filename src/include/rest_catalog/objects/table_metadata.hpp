
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/encrypted_key.hpp"
#include "rest_catalog/objects/metadata_log.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/snapshot.hpp"
#include "rest_catalog/objects/snapshot_log.hpp"
#include "rest_catalog/objects/snapshot_references.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {



class TableMetadata {
public:
	TableMetadata();
	TableMetadata(const TableMetadata&) = delete;
	TableMetadata& operator=(const TableMetadata&) = delete;
	TableMetadata(TableMetadata&&) = default;
	TableMetadata &operator=(TableMetadata&&) = default;
public:
	static TableMetadata FromJSON(yyjson_val *obj);
	TableMetadata Copy() const;
public:
	string TryFromJSON(yyjson_val *obj);
public:
	int32_t format_version;
	string table_uuid;
	string location;
	bool has_location;
	int64_t last_updated_ms;
	bool has_last_updated_ms;
	int64_t next_row_id;
	bool has_next_row_id;
	case_insensitive_map_t<string> properties;
	bool has_properties;
	vector<Schema> schemas;
	bool has_schemas;
	int32_t current_schema_id;
	bool has_current_schema_id;
	int32_t last_column_id;
	bool has_last_column_id;
	vector<PartitionSpec> partition_specs;
	bool has_partition_specs;
	int32_t default_spec_id;
	bool has_default_spec_id;
	int32_t last_partition_id;
	bool has_last_partition_id;
	vector<SortOrder> sort_orders;
	bool has_sort_orders;
	int32_t default_sort_order_id;
	bool has_default_sort_order_id;
	vector<EncryptedKey> encryption_keys;
	bool has_encryption_keys;
	vector<Snapshot> snapshots;
	bool has_snapshots;
	SnapshotReferences refs;
	bool has_refs;
	int64_t current_snapshot_id;
	bool has_current_snapshot_id;
	int64_t last_sequence_number;
	bool has_last_sequence_number;
	SnapshotLog snapshot_log;
	bool has_snapshot_log;
	MetadataLog metadata_log;
	bool has_metadata_log;
	vector<StatisticsFile> statistics;
	bool has_statistics;
	vector<PartitionStatisticsFile> partition_statistics;
	bool has_partition_statistics;
};

} // namespace rest_api_objects
} // namespace duckdb

