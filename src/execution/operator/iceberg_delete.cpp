#include "execution/operator/iceberg_delete.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

#include "core/deletes/iceberg_deletion_vector.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

namespace duckdb {
class IcebergDeleteLocalState;
class IcebergDeleteGlobalState;
class IcebergTableEntry;

IcebergDelete::IcebergDelete(PhysicalPlan &physical_plan, IcebergTableEntry &table,
                             IcebergMultiFileList &multi_file_list, PhysicalOperator &child,
                             vector<idx_t> row_id_indexes, bool is_equality_delete,
                             vector<IcebergEqualityDeletePredicate> equality_predicates)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      multi_file_list(multi_file_list), row_id_indexes(std::move(row_id_indexes)),
      is_equality_delete(is_equality_delete), equality_predicates(std::move(equality_predicates)) {
	children.push_back(child);
}

unique_ptr<GlobalSinkState> IcebergDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergDeleteGlobalState>();
}

unique_ptr<LocalSinkState> IcebergDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<IcebergDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType IcebergDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	if (is_equality_delete) {
		//! The equality-delete file's contents come entirely from the planning-time predicates,
		//! not from the streamed rows - write it exactly once and stop consuming input.
		bool should_write = false;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (!global_state.equality_delete_written) {
				global_state.equality_delete_written = true;
				should_write = true;
			}
		}
		if (should_write) {
			WriteEqualityDeleteFile(context.client, global_state);
		}
		return SinkResultType::FINISHED;
	}
#endif

	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_row_number = chunk.data[row_id_indexes[1]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(chunk.size(), row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(chunk.size(), file_name_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto row_idx = row_data.sel->get_index(i);
		auto file_name_idx = file_name_vdata.sel->get_index(i);
		if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
		auto file_name = file_name_data[file_name_idx].GetString();

		if (local_state.current_file_name.empty() || local_state.current_file_name != file_name) {
			// local_state points to new file, flush to global state
			global_state.Flush(local_state);
			local_state.current_file_name = file_name;
		}
		auto row_number = file_row_data[row_idx];
		local_state.file_row_numbers.push_back(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType IcebergDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();
	global_state.FinalFlush(local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

void IcebergDelete::WriteDeletionVectorFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
                                            const string &filename, IcebergDeleteFileInfo delete_file,
                                            const set<idx_t> &sorted_deletes) const {
	auto delete_file_path = delete_file.file_name;

	// Build deletion vector data
	unordered_map<int32_t, roaring::Roaring> bitmaps;

	// Group row indices by high 32 bits
	for (auto row_idx : sorted_deletes) {
		int64_t row_id = static_cast<int64_t>(row_idx);
		int32_t high_bits = static_cast<int32_t>(row_id >> 32);
		uint32_t low_bits = static_cast<uint32_t>(row_id & 0xFFFFFFFF);

		auto &bitmap = bitmaps[high_bits];
		bitmap.add(low_bits);
	}

	// Serialize to blob
	auto blob_data = IcebergDeletionVectorData::ToBlob(bitmaps);

	// Write blob to file
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_handle =
	    fs.OpenFile(delete_file_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	file_handle->Write(blob_data.data(), blob_data.size());
	file_handle->Close();

	delete_file.file_name = delete_file_path;
	delete_file.file_format = "puffin";
	delete_file.delete_count = sorted_deletes.size();
	delete_file.content_offset = 0;
	delete_file.content_size_in_bytes = blob_data.size();
	delete_file.file_size_bytes = delete_file.content_size_in_bytes.GetIndex();
	global_state.written_files.emplace(filename, std::move(delete_file));
}

void IcebergDelete::WritePositionalDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
                                              const string &filename, IcebergDeleteFileInfo delete_file,
                                              set<idx_t> sorted_deletes) const {
	auto delete_file_path = delete_file.file_name;
	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs ids and names for the delete files
	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::DELETE_FILE_PATH_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::DELETE_POS_FIELD_ID));
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	// generate the physical copy to file
	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(context));

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// run the copy to file
	vector<LogicalType> write_types;
	write_types.push_back(LogicalType::VARCHAR);
	write_types.push_back(LogicalType::BIGINT);

	DataChunk write_chunk;
	write_chunk.Initialize(context, write_types);
	// the first vector is constant (the file name)
	Value filename_val(filename);
	write_chunk.data[0].Reference(filename_val);

	idx_t row_count = 0;
	auto row_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	for (auto &row_idx : sorted_deletes) {
		row_data[row_count++] = NumericCast<int64_t>(row_idx);
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetCardinality(row_count);
			copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
			                               write_chunk);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetCardinality(row_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = stats.row_count;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.footer_size = stats.footer_size_bytes.GetValue<idx_t>();
	auto pos_stats = stats.column_statistics.find("\"pos\"");
	auto pos_min = pos_stats->second.find("min");
	auto pos_min_value = pos_min->second.GetValue<idx_t>();
	auto pos_max = pos_stats->second.find("max");
	auto pos_max_value = pos_max->second.GetValue<idx_t>();
	delete_file.pos_min_value = pos_min_value;
	delete_file.pos_max_value = pos_max_value;
	global_state.written_files.emplace(filename, std::move(delete_file));
}

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
void IcebergDelete::WriteEqualityDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state) const {
	D_ASSERT(!equality_predicates.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = table.table_info.table_metadata.GetDataPath(fs);
	string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-equality-deletes.parquet";
	string delete_file_path = fs.JoinPath(data_path, delete_filename);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// Generate the field ids for the parquet writer: every column carries, as PARQUET:field_id
	// metadata, the iceberg field-id that the equality delete applies to.
	child_list_t<Value> field_id_values;
	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	vector<int32_t> equality_ids;
	for (auto &predicate : equality_predicates) {
		field_id_values.emplace_back(predicate.column_name, Value::INTEGER(predicate.field_id));
		names_to_write.push_back(predicate.column_name);
		types_to_write.push_back(predicate.type);
		equality_ids.push_back(predicate.field_id);
	}
	vector<Value> field_input;
	field_input.push_back(Value::STRUCT(std::move(field_id_values)));
	info->options["field_ids"] = std::move(field_input);

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// Write a single row containing the equality-delete tuple (one value per equality column).
	DataChunk write_chunk;
	write_chunk.Initialize(context, types_to_write);
	for (idx_t col_idx = 0; col_idx < equality_predicates.size(); col_idx++) {
		write_chunk.data[col_idx].SetValue(0, equality_predicates[col_idx].value);
	}
	write_chunk.SetCardinality(1);
	copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
	                               write_chunk);

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	IcebergDeleteFileInfo delete_file;
	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = 1;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.equality_ids = std::move(equality_ids);
	global_state.written_files.emplace(delete_file_path, std::move(delete_file));
}
#endif

static void PopulateAlteredManifests(const IcebergMultiFileList &multi_file_list, IcebergManifestDeletes &out,
                                     IcebergDeleteData &delete_data) {
	if (delete_data.type != IcebergDeleteType::DELETION_VECTOR) {
		return;
	}
	for (auto &bound_entry : delete_data.entries) {
		auto &entry = bound_entry.entry;
		out.InvalidateFile(entry.data_file.file_path);
	}
}

void IcebergDelete::FlushDeletes(IcebergTransaction &transaction, ClientContext &context,
                                 IcebergDeleteGlobalState &global_state) const {
	bool write_deletion_vector = table.table_info.table_metadata.iceberg_version >= 3;

	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : global_state.deleted_rows) {
		auto &filename = entry.first;
		auto &deleted_rows = entry.second;

		// sort and duplicate eliminate the deletes
		set<idx_t> sorted_deletes;
		for (auto &row_idx : deleted_rows) {
			sorted_deletes.insert(row_idx);
		}
		if (sorted_deletes.size() != deleted_rows.size()) {
			throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
			                              "Iceberg. Eliminate duplicate matches prior to running the UPDATE");
		}
		if (write_deletion_vector) {
			//! Addd the existing delete we're replacing
			auto it = multi_file_list.positional_delete_data.find(filename);
			if (it != multi_file_list.positional_delete_data.end()) {
				auto &delete_data = *it->second;
				PopulateAlteredManifests(multi_file_list, global_state.altered_manifests, delete_data);
				delete_data.ToSet(sorted_deletes);
			}
		}

		IcebergDeleteFileInfo delete_file;
		delete_file.data_file_path = filename;
		delete_file.partition_info = multi_file_list.GetPartitionInfoForDataFile(filename);

		auto &fs = FileSystem::GetFileSystem(context);

		string file_format;
		if (write_deletion_vector) {
			file_format = "puffin";
		} else {
			file_format = "parquet";
		}

		string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes." + file_format;
		// Place the delete file in the same directory as the data file it references,
		// so that for partitioned tables it lands in the correct partition folder.
		auto sep = fs.PathSeparator(filename);
		auto last_sep = filename.rfind(sep);
		if (last_sep == string::npos) {
			throw InvalidConfigurationException("Cannot create valid file path for delete file");
		}
		string data_file_dir = filename.substr(0, last_sep);
		string delete_file_path = fs.JoinPath(data_file_dir, delete_filename);

		delete_file.file_name = delete_file_path;

		if (!write_deletion_vector) {
			WritePositionalDeleteFile(context, global_state, filename, delete_file, sorted_deletes);
		} else {
			WriteDeletionVectorFile(context, global_state, filename, delete_file, sorted_deletes);
		}
	}
}

vector<IcebergManifestEntry> IcebergDelete::GenerateDeleteManifestEntries(IcebergDeleteGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	auto &delete_files = global_state.written_files;
	vector<IcebergManifestEntry> iceberg_delete_files;
	for (auto &delete_entry : delete_files) {
		auto data_file_name = delete_entry.first;
		auto &delete_file = delete_entry.second;

		IcebergManifestEntry manifest_entry;
		manifest_entry.status = IcebergManifestEntryStatusType::ADDED;
		auto &data_file = manifest_entry.data_file;

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
		if (!delete_file.equality_ids.empty()) {
			//! Equality delete: a global delete identified only by the equality field values.
			//! It has no referenced data file, no filename bounds and no partition info.
			data_file.content = IcebergManifestEntryContentType::EQUALITY_DELETES;
			data_file.file_path = delete_file.file_name;
			data_file.file_format = delete_file.file_format;
			data_file.record_count = delete_file.delete_count;
			data_file.file_size_in_bytes = delete_file.file_size_bytes;
			data_file.equality_ids = delete_file.equality_ids;
			iceberg_delete_files.push_back(manifest_entry);
			continue;
		}
#endif

		data_file.content = IcebergManifestEntryContentType::POSITION_DELETES;
		data_file.file_path = delete_file.file_name;
		data_file.file_format = delete_file.file_format;
		data_file.record_count = delete_file.delete_count;
		data_file.file_size_in_bytes = delete_file.file_size_bytes;
		if (delete_file.content_size_in_bytes.IsValid()) {
			data_file.content_size_in_bytes = Value::BIGINT(delete_file.content_size_in_bytes.GetIndex());
		}
		if (delete_file.content_offset.IsValid()) {
			data_file.content_offset = Value::BIGINT(delete_file.content_offset.GetIndex());
		}

		// set lower and upper bound for the filename column
		data_file.lower_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
		data_file.upper_bounds[MultiFileReader::FILENAME_FIELD_ID] = Value::BLOB(data_file_name);
		// set referenced_data_file
		data_file.referenced_data_file = data_file_name;
		// copy partition info from the data file being deleted
		data_file.partition_info = delete_file.partition_info;
		iceberg_delete_files.push_back(manifest_entry);
	}
	return iceberg_delete_files;
}

SinkFinalizeType IcebergDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	if (is_equality_delete) {
		//! Ensure the equality-delete file is written even if Sink never ran (e.g. zero matching rows).
		bool should_write = false;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (!global_state.equality_delete_written) {
				global_state.equality_delete_written = true;
				should_write = true;
			}
		}
		if (should_write) {
			WriteEqualityDeleteFile(context, global_state);
		}
	} else if (global_state.deleted_rows.empty()) {
		// FIXME: replace with get deleted rows
		return SinkFinalizeType::READY;
	}
#else
	// FIXME: replace with get deleted rows
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}
#endif

	auto &iceberg_transaction = IcebergTransaction::Get(context, table.catalog);
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	if (!is_equality_delete) {
		FlushDeletes(iceberg_transaction, context, global_state);
	}
#else
	// write out the delete rows
	FlushDeletes(iceberg_transaction, context, global_state);
#endif

	// write out the new manifest file
	auto &irc_table = table.Cast<IcebergTableEntry>();

	auto &table_info = irc_table.table_info;
	auto iceberg_delete_files = GenerateDeleteManifestEntries(global_state);

	if (!global_state.written_files.empty()) {
		ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTableInformation &tbl) {
			auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
			transaction_data.AddSnapshot(IcebergSnapshotOperationType::DELETE, std::move(iceberg_delete_files),
			                             std::move(global_state.altered_manifests));

			//! Add or overwrite the currently active transaction-local delete files
			for (auto &entry : global_state.written_files) {
				auto &delete_file = entry.second;
				if (table_info.table_metadata.iceberg_version >= 3) {
					transaction_data.transactional_delete_files[delete_file.data_file_path] = delete_file.file_name;
				}
			}
		});
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count.load()));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergDelete::GetName() const {
	return "ICEBERG_DELETE";
}

InsertionOrderPreservingMap<string> IcebergDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

static optional_ptr<PhysicalTableScan> FindDeleteSource(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();
		bool found = false;
		for (auto &col : scan.column_ids) {
			if (col.GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
		return scan;
	}
	for (auto &children : plan.children) {
		auto result = FindDeleteSource(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
static bool PlanContainsPhysicalFilter(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::FILTER) {
		return true;
	}
	for (auto &child : plan.children) {
		if (PlanContainsPhysicalFilter(child.get())) {
			return true;
		}
	}
	return false;
}

bool IcebergDelete::TryGetEqualityDeletePredicates(ClientContext &context, IcebergTableEntry &table,
                                                   PhysicalOperator &child_plan,
                                                   vector<IcebergEqualityDeletePredicate> &equality_predicates) {
	//! Gated behind an explicit testing-only setting.
	Value setting_value;
	if (!context.TryGetCurrentSetting(ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE, setting_value) ||
	    setting_value.IsNull() || !setting_value.GetValue<bool>()) {
		return false;
	}

	//! Equality-delete writing is only supported for v2, unpartitioned tables.
	auto &table_metadata = table.table_info.table_metadata;
	if (table_metadata.iceberg_version != 2) {
		return false;
	}
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		return false;
	}

	//! Any filter means this cannot be an equality delete.
	if (PlanContainsPhysicalFilter(child_plan)) {
		return false;
	}

	auto table_scan = FindDeleteSource(child_plan);
	if (!table_scan) {
		return false;
	}
	auto &scan = *table_scan;
	if (!scan.table_filters || scan.table_filters->filters.empty()) {
		return false;
	}

	auto &schema = table_metadata.GetLatestSchema();
	auto &columns = schema.columns;
	for (auto &filter_entry : scan.table_filters->filters) {
		auto column_key = filter_entry.first;
		auto &table_filter = *filter_entry.second;
		//! Only a plain `column = constant` qualifies (rejects IN, OR/AND conjunctions, IS NULL, ...).
		if (table_filter.filter_type != TableFilterType::CONSTANT_COMPARISON) {
			return false;
		}
		auto &constant_filter = table_filter.Cast<ConstantFilter>();
		if (constant_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		if (column_key >= scan.column_ids.size()) {
			return false;
		}
		auto &column_index = scan.column_ids[column_key];
		if (column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= columns.size()) {
			return false;
		}
		auto &column_definition = *columns[primary_index];
		//! The same column referenced more than once is not a clean equality delete.
		for (auto &existing : equality_predicates) {
			if (existing.field_id == column_definition.id) {
				return false;
			}
		}
		Value delete_value;
		string error_message;
		if (!constant_filter.constant.DefaultTryCastAs(column_definition.type, delete_value, &error_message, true)) {
			return false;
		}
		IcebergEqualityDeletePredicate predicate;
		predicate.field_id = column_definition.id;
		predicate.column_name = column_definition.name;
		predicate.type = column_definition.type;
		predicate.value = std::move(delete_value);
		equality_predicates.push_back(std::move(predicate));
	}
	return !equality_predicates.empty();
}
#endif

PhysicalOperator &IcebergDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                            IcebergTableEntry &table, PhysicalOperator &child_plan,
                                            vector<idx_t> row_id_indexes) {
	auto table_scan = FindDeleteSource(child_plan);
	if (!table_scan) {
		throw InternalException("Couldn't locate the scan that feeds the delete information");
	}
	auto &bind_data = table_scan->bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();

	vector<IcebergEqualityDeletePredicate> equality_predicates;
	bool is_equality_delete = false;
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	is_equality_delete = TryGetEqualityDeletePredicates(context, table, child_plan, equality_predicates);
#endif

	return planner.Make<IcebergDelete>(table, file_list, child_plan, std::move(row_id_indexes), is_equality_delete,
	                                   std::move(equality_predicates));
}

PhysicalOperator &IcebergCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from Iceberg table");
	}
	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	auto iceberg_version = updated_table_entry.table_info.table_metadata.iceberg_version;
	if (iceberg_version < 2) {
		throw NotImplementedException("Delete from Iceberg V%d tables",
		                              updated_table_entry.table_info.table_metadata.iceberg_version);
	}

	vector<idx_t> row_id_indexes;
	// we only push 2 columns for positional deletes
	idx_t column_offset = 0;
	if (iceberg_version >= 3) {
		//! The row ids of the table contain the _row_id column, which we're not interested in
		column_offset = 1;
	}
	for (idx_t i = 0; i < 2; i++) {
		auto &bound_ref = op.expressions[column_offset + i]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.index);
	}

	auto allows_positional_deletes = updated_table_entry.table_info.table_metadata.PropertiesAllowPositionalDeletes(
	    IcebergSnapshotOperationType::DELETE);
	if (!allows_positional_deletes) {
		auto delete_table_property = updated_table_entry.table_info.table_metadata.GetTableProperty(WRITE_DELETE_MODE);
		auto error_message = IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(
		    updated_table_entry.name, WRITE_DELETE_MODE, delete_table_property);
		throw NotImplementedException(error_message);
	}

	auto &iceberg_delete = IcebergDelete::PlanDelete(context, planner, updated_table_entry, plan, row_id_indexes);
	return iceberg_delete;
}

} // namespace duckdb
