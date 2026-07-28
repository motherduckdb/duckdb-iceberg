//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

struct IcebergEqualityDeleteColumn {
	int32_t field_id;
	ColumnIndex column_index;
	//! Index in the query projection, or INVALID_INDEX when this is a private equality-delete column
	idx_t projected_expression_index;
	LogicalType type;
};

struct IcebergEqualityDeleteReadState {
	vector<idx_t> expression_indexes;
	unique_ptr<Expression> expression;
};

struct IcebergMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
public:
	IcebergMultiFileReaderGlobalState(bool supports_local_extra_columns, const MultiFileList &file_list_p,
	                                  vector<MultiFileColumnDefinition> scan_columns_p,
	                                  vector<ColumnIndex> scan_column_ids_p,
	                                  vector<IcebergEqualityDeleteColumn> equality_delete_columns_p)
	    : MultiFileReaderGlobalState({}, file_list_p, supports_local_extra_columns),
	      scan_columns(std::move(scan_columns_p)), scan_column_ids(std::move(scan_column_ids_p)),
	      equality_delete_columns(std::move(equality_delete_columns_p)) {
		for (idx_t i = 0; i < equality_delete_columns.size(); i++) {
			equality_delete_field_indexes.emplace(equality_delete_columns[i].field_id, i);
			equality_delete_types.push_back(equality_delete_columns[i].type);
		}
	}

	void CacheEqualityDeleteExpressionIndexes(idx_t file_list_idx, vector<idx_t> expression_indexes) {
		lock_guard<mutex> guard(equality_delete_read_state_lock);
		auto &state = equality_delete_read_states[file_list_idx];
		if (!state) {
			state = make_uniq<IcebergEqualityDeleteReadState>();
		}
		state->expression_indexes = std::move(expression_indexes);
	}

	void CacheEqualityDeleteExpression(idx_t file_list_idx, unique_ptr<Expression> expression) {
		lock_guard<mutex> guard(equality_delete_read_state_lock);
		auto &state = equality_delete_read_states[file_list_idx];
		if (!state) {
			state = make_uniq<IcebergEqualityDeleteReadState>();
		}
		state->expression = std::move(expression);
	}

	const IcebergEqualityDeleteReadState &GetEqualityDeleteReadState(idx_t file_list_idx) const {
		lock_guard<mutex> guard(equality_delete_read_state_lock);
		auto entry = equality_delete_read_states.find(file_list_idx);
		if (entry == equality_delete_read_states.end()) {
			throw InternalException("Equality-delete state was not initialized for file-list index %llu",
			                        file_list_idx);
		}
		return *entry->second;
	}

public:
	vector<MultiFileColumnDefinition> scan_columns;
	vector<ColumnIndex> scan_column_ids;
	vector<IcebergEqualityDeleteColumn> equality_delete_columns;
	vector<LogicalType> equality_delete_types;
	//! field_id -> equality_delete_columns index
	unordered_map<int32_t, idx_t> equality_delete_field_indexes;

private:
	mutable mutex equality_delete_read_state_lock;
	//! The values are heap allocated so references remain stable while other files are initialized in parallel.
	unordered_map<idx_t, unique_ptr<IcebergEqualityDeleteReadState>> equality_delete_read_states;
};

struct IcebergMultiFileReader : public MultiFileReader {
public:
	static constexpr column_t COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER = UINT64_C(10000000000000000000);

public:
	IcebergMultiFileReader(shared_ptr<TableFunctionInfo> function_info);

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);
	static vector<PartitionStatistics> IcebergGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input);

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;
	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  MultiFileGlobalState &gstate);
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;
	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;
	bool ParseOption(const string &key, const Value &val, MultiFileOptions &options, ClientContext &context) override;

	MultiFileReaderVirtualColumnBinding
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, const idx_t column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_idx) override;

private:
	static unique_ptr<Expression> CreateEqualityDeleteExpression(const IcebergMultiFileList &multi_file_list,
	                                                             const BoundIcebergManifestEntry &bound_manifest_entry,
	                                                             const vector<MultiFileColumnDefinition> &local_columns,
	                                                             const IcebergMultiFileReaderGlobalState &global_state);
	static void ApplyPartitionConstants(const IcebergMultiFileList &multi_file_list, MultiFileReaderData &reader_data,
	                                    const vector<MultiFileColumnDefinition> &global_columns,
	                                    const vector<ColumnIndex> &global_column_ids, ClientContext &context);

public:
	shared_ptr<TableFunctionInfo> function_info;
	IcebergOptions options;

private:
	unique_ptr<MultiFileColumnDefinition> row_id_column;
	unique_ptr<MultiFileColumnDefinition> last_updated_sequence_number_column;
};

} // namespace duckdb
