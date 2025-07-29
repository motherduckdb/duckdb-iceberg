//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_metadata_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/index.hpp"
#include "common/iceberg_data_file.hpp"
// #include "common/iceberg_name_map.hpp"

namespace duckdb {

struct IcebergTag {
	string key;
	string value;
};

struct IcebergSchemaSetting {
	SchemaIndex schema_id;
	IcebergTag tag;
};

struct IcebergTableSetting {
	TableIndex table_id;
	IcebergTag tag;
};

struct IcebergMetadata {
	vector<IcebergTag> tags;
	vector<IcebergSchemaSetting> schema_settings;
	vector<IcebergTableSetting> table_settings;
};

struct IcebergSchemaInfo {
	SchemaIndex id;
	string uuid;
	string name;
	string path;
	vector<IcebergTag> tags;
};

struct IcebergColumnInfo {
	FieldIndex id;
	string name;
	string type;
	Value initial_default;
	Value default_value;
	bool nulls_allowed {};
	vector<IcebergColumnInfo> children;
	vector<IcebergTag> tags;
};

struct IcebergInlinedTableInfo {
	string table_name;
	idx_t schema_version;
};

struct IcebergTableInfo {
	TableIndex id;
	SchemaIndex schema_id;
	string uuid;
	string name;
	string path;
	vector<IcebergColumnInfo> columns;
	vector<IcebergTag> tags;
	vector<IcebergInlinedTableInfo> inlined_data_tables;
};

struct IcebergColumnStatsInfo {
	FieldIndex column_id;
	string value_count;
	string null_count;
	string column_size_bytes;
	string min_val;
	string max_val;
	string contains_nan;
};

struct IcebergFilePartitionInfo {
	idx_t partition_column_idx;
	string partition_value;
};

struct IcebergPartialFileInfo {
	idx_t snapshot_id;
	idx_t max_row_count;
};

struct IcebergFileInfo {
	DataFileIndex id;
	TableIndex table_id;
	string file_name;
	idx_t row_count;
	idx_t file_size_bytes;
	optional_idx footer_size;
	optional_idx row_id_start;
	optional_idx partition_id;
	optional_idx begin_snapshot;
	optional_idx max_partial_file_snapshot;
	string encryption_key;
	MappingIndex mapping_id;
	vector<IcebergColumnStatsInfo> column_stats;
	vector<IcebergFilePartitionInfo> partition_values;
	vector<IcebergPartialFileInfo> partial_file_info;
};

// struct IcebergInlinedDataInfo {
// 	TableIndex table_id;
// 	idx_t row_id_start;
// 	optional_ptr<IcebergInlinedData> data;
// };

struct IcebergDeletedInlinedDataInfo {
	TableIndex table_id;
	string table_name;
	vector<idx_t> deleted_row_ids;
};

struct IcebergDeleteFileInfo {
	DataFileIndex id;
	TableIndex table_id;
	DataFileIndex data_file_id;
	string path;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	string encryption_key;
};

struct IcebergPartitionFieldInfo {
	idx_t partition_key_index = 0;
	FieldIndex field_id;
	string transform;
};

struct IcebergPartitionInfo {
	optional_idx id;
	TableIndex table_id;
	vector<IcebergPartitionFieldInfo> fields;
};

struct IcebergGlobalColumnStatsInfo {
	FieldIndex column_id;

	bool contains_null = false;
	bool has_contains_null = false;

	bool contains_nan = false;
	bool has_contains_nan = false;

	string min_val;
	bool has_min = false;

	string max_val;
	bool has_max = false;
};

struct IcebergGlobalStatsInfo {
	TableIndex table_id;
	bool initialized;
	idx_t record_count;
	idx_t next_row_id;
	idx_t table_size_bytes;
	vector<IcebergGlobalColumnStatsInfo> column_stats;
};

struct SnapshotChangeInfo {
	string changes_made;
};

struct IcebergSnapshotInfo {
	idx_t id;
	timestamp_tz_t time;
	idx_t schema_version;
	SnapshotChangeInfo change_info;
};

struct IcebergViewInfo {
	TableIndex id;
	SchemaIndex schema_id;
	string uuid;
	string name;
	string dialect;
	vector<string> column_aliases;
	string sql;
	vector<IcebergTag> tags;
};

struct IcebergTagInfo {
	idx_t id;
	string key;
	Value value;
};

struct IcebergColumnTagInfo {
	TableIndex table_id;
	FieldIndex field_index;
	string key;
	Value value;
};

struct IcebergDroppedColumn {
	TableIndex table_id;
	FieldIndex field_id;
};

struct IcebergNewColumn {
	TableIndex table_id;
	IcebergColumnInfo column_info;
	optional_idx parent_idx;
};

struct IcebergCatalogInfo {
	vector<IcebergSchemaInfo> schemas;
	vector<IcebergTableInfo> tables;
	vector<IcebergViewInfo> views;
	vector<IcebergPartitionInfo> partitions;
};

struct IcebergFileData {
	string path;
	idx_t file_size_bytes = 0;
	optional_idx footer_size;
};

enum class IcebergDataType {
	DATA_FILE,
	INLINED_DATA,
	TRANSACTION_LOCAL_INLINED_DATA,
};

struct IcebergFileListEntry {
	IcebergFileData file;
	IcebergFileData delete_file;
	optional_idx row_id_start;
	optional_idx snapshot_id;
	optional_idx max_row_count;
	optional_idx snapshot_filter;
	MappingIndex mapping_id;
	IcebergDataType data_type = IcebergDataType::DATA_FILE;
};

struct IcebergDeleteScanEntry {
	IcebergFileData file;
	IcebergFileData delete_file;
	IcebergFileData previous_delete_file;
	idx_t row_count;
	optional_idx row_id_start;
	MappingIndex mapping_id;
	optional_idx snapshot_id;
};

struct IcebergFileListExtendedEntry {
	DataFileIndex file_id;
	DataFileIndex delete_file_id;
	IcebergFileData file;
	IcebergFileData delete_file;
	optional_idx row_id_start;
	optional_idx snapshot_id;
	idx_t row_count;
	idx_t delete_count = 0;
	IcebergDataType data_type = IcebergDataType::DATA_FILE;
};

struct IcebergCompactionBaseFileData {
	DataFileIndex id;
	IcebergFileData data;
	idx_t row_count = 0;
	idx_t begin_snapshot = 0;
	optional_idx end_snapshot;
	optional_idx max_row_count;
};

struct IcebergFileScheduledForCleanup {
	DataFileIndex id;
	string path;
	timestamp_tz_t time;
};

struct IcebergCompactionFileData : public IcebergCompactionBaseFileData {
	optional_idx row_id_start;
	MappingIndex mapping_id;
	optional_idx partition_id;
	vector<string> partition_values;
};

struct IcebergCompactionDeleteFileData : public IcebergCompactionBaseFileData {};

struct IcebergCompactionFileEntry {
	IcebergCompactionFileData file;
	vector<IcebergCompactionDeleteFileData> delete_files;
	vector<IcebergPartialFileInfo> partial_files;
	idx_t schema_version;
};

// struct IcebergCompactionEntry {
// 	vector<IcebergCompactionFileEntry> source_files;
// 	IcebergDataFile written_file;
// 	optional_idx row_id_start;
// };

// struct IcebergCompactedFileInfo {
// 	string path;
// 	DataFileIndex source_id;
// 	DataFileIndex new_id;
// };

// struct IcebergTableSizeInfo {
// 	SchemaIndex schema_id;
// 	TableIndex table_id;
// 	string table_name;
// 	string table_uuid;
// 	idx_t file_size_bytes = 0;
// 	idx_t delete_file_size_bytes = 0;
// 	idx_t file_count = 0;
// 	idx_t delete_file_count = 0;
// };

struct IcebergPath {
	string path;
	bool path_is_relative;
};

// struct IcebergConfigOption {
// 	IcebergTag option;
// 	//! schema_id, if scoped to a schema
// 	SchemaIndex schema_id;
// 	//! table_id, if scoped to a table
// 	TableIndex table_id;
// };

struct IcebergNameMapColumnInfo {
	idx_t column_id;
	string source_name;
	FieldIndex target_field_id;
	bool hive_partition = false;
	optional_idx parent_column;
};

struct IcebergColumnMappingInfo {
	TableIndex table_id;
	MappingIndex mapping_id;
	string map_type;
	vector<IcebergNameMapColumnInfo> map_columns;
};

} // namespace duckdb
