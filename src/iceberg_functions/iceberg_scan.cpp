#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_multi_file_reader.hpp"
#include "iceberg_functions.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "metadata/iceberg_column_definition.hpp"
#include "metadata/iceberg_predicate_stats.hpp"
#include "iceberg_value.hpp"

#include <string>
#include <numeric>
#include <unordered_map>

namespace duckdb {

struct IcebergPartitionRowGroup : public PartitionRowGroup {
	//! Schema columns for mapping column_index -> field_id
	const vector<unique_ptr<IcebergColumnDefinition>> &schema;
	//! Reference to the data file with bounds
	const IcebergDataFile &data_file;

	IcebergPartitionRowGroup(const vector<unique_ptr<IcebergColumnDefinition>> &schema,
	                         const IcebergDataFile &data_file)
	    : schema(schema), data_file(data_file) {
	}

	unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override {
		auto col_idx = storage_index.GetPrimaryIndex();
		if (col_idx >= schema.size()) {
			return nullptr;
		}
		auto &column = *schema[col_idx];
		auto field_id = column.id;

		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		if (lower_it == data_file.lower_bounds.end() && upper_it == data_file.upper_bounds.end()) {
			return nullptr;
		}

		Value lower_bound, upper_bound;
		if (lower_it != data_file.lower_bounds.end()) {
			lower_bound = lower_it->second;
		}
		if (upper_it != data_file.upper_bounds.end()) {
			upper_bound = upper_it->second;
		}

		// DeserializeBounds converts from BLOB to typed values
		IcebergPredicateStats pred_stats;
		try {
			pred_stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);
		} catch (...) {
			return nullptr;
		}

		if (!pred_stats.has_lower_bounds && !pred_stats.has_upper_bounds) {
			return nullptr;
		}

		auto &type = column.type;
		if (type.IsNumeric() || type.IsTemporal()) {
			auto stats = NumericStats::CreateEmpty(type);
			if (pred_stats.has_lower_bounds) {
				NumericStats::SetMin(stats, pred_stats.lower_bound);
			}
			if (pred_stats.has_upper_bounds) {
				NumericStats::SetMax(stats, pred_stats.upper_bound);
			}
			return stats.ToUnique();
		} else if (type.id() == LogicalTypeId::VARCHAR) {
			auto stats = StringStats::CreateEmpty(type);
			if (pred_stats.has_lower_bounds) {
				StringStats::SetMin(stats, pred_stats.lower_bound.GetValueUnsafe<string_t>());
			}
			if (pred_stats.has_upper_bounds) {
				StringStats::SetMax(stats, pred_stats.upper_bound.GetValueUnsafe<string_t>());
			}
			return stats.ToUnique();
		}
		return nullptr;
	}

	bool MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) override {
		// File-level bounds are not exact (they are min/max across the file, not single values)
		// This is fine for partition pruning but not for eager MIN/MAX aggregates
		return false;
	}
};

static void AddNamedParameters(TableFunction &fun) {
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
}

virtual_column_map_t IcebergVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto result = IcebergTableEntry::VirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

static void IcebergScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                 const TableFunction &function) {
	throw NotImplementedException("IcebergScan serialization not implemented");
}

BindInfo IcebergBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	if (!file_list.table) {
		return BindInfo(ScanType::EXTERNAL);
	}
	return BindInfo(*file_list.table);
}

vector<PartitionStatistics> IcebergGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	vector<PartitionStatistics> result;
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();

	// V1 tables lack reliable per-file row counts
	if (file_list.GetMetadata().iceberg_version == 1) {
		return result;
	}

	// Force all files to be loaded
	(void)file_list.GetTotalFileCount();

	// Build per-data-file delete counts from delete manifest entries.
	// For positional deletes with a referenced_data_file (deletion vectors and optimized V2
	// positional deletes), we know exactly which data file is targeted and how many rows are
	// deleted, so we can compute exact net counts from manifest metadata alone.
	std::unordered_map<string, int64_t> deletes_per_file;
	bool has_inexact_deletes = false;

	if (!file_list.delete_manifests.empty()) {
		auto iceberg_path = file_list.GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		auto &options = file_list.options;
		auto &metadata = file_list.GetMetadata();
		auto &snapshot = *file_list.GetSnapshot();

		// Re-scan delete manifests independently (don't consume the shared delete_manifest_reader)
		auto delete_scan =
		    AvroScan::ScanManifest(snapshot, file_list.delete_manifests, options, fs, iceberg_path, metadata, context);
		manifest_file::ManifestReader reader(*delete_scan, true);

		vector<IcebergManifestEntry> entries;
		while (!reader.Finished()) {
			entries.clear();
			reader.Read(STANDARD_VECTOR_SIZE, entries);
			for (auto &e : entries) {
				if (e.data_file.content == IcebergManifestEntryContentType::POSITION_DELETES &&
				    !e.data_file.referenced_data_file.empty()) {
					deletes_per_file[e.data_file.referenced_data_file] += e.data_file.record_count;
				} else {
					// Equality deletes or V2 positional without a specific target — genuinely approximate
					has_inexact_deletes = true;
				}
			}
		}
	}

	auto &schema = file_list.GetSchema().columns;

	for (auto &entry : file_list.manifest_entries) {
		PartitionStatistics stats;
		int64_t deleted = 0;
		auto it = deletes_per_file.find(entry.data_file.file_path);
		if (it != deletes_per_file.end()) {
			deleted = it->second;
		}
		stats.count = NumericCast<idx_t>(entry.data_file.record_count - deleted);
		stats.count_type = has_inexact_deletes ? CountType::COUNT_APPROXIMATE : CountType::COUNT_EXACT;
		if (!entry.data_file.lower_bounds.empty() || !entry.data_file.upper_bounds.empty()) {
			stats.partition_row_group = make_shared_ptr<IcebergPartitionRowGroup>(schema, entry.data_file);
		}
		result.push_back(std::move(stats));
	}
	return result;
}

TableFunctionSet IcebergFunctions::GetIcebergScanFunction(ExtensionLoader &loader) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read

	auto &parquet_scan = loader.GetTableFunction("parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergMultiFileReader::CreateInstance;
		function.late_materialization = false;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = IcebergScanSerialize;
		function.deserialize = nullptr;

		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = IcebergBindInfo;
		function.get_virtual_columns = IcebergVirtualColumns;
		function.get_partition_stats = IcebergGetPartitionStats;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		AddNamedParameters(function);

		function.name = "iceberg_scan";
	}

	parquet_scan_copy.name = "iceberg_scan";
	return parquet_scan_copy;
}

} // namespace duckdb
