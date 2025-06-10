#include "manifest_reader.hpp"
#include "iceberg_manifest.hpp"
#include "iceberg_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/database.hpp"

//! Iceberg Manifest scan routines

namespace duckdb {

static void ManifestNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	name_to_vec[name] = ColumnIndex(column_id);
}

template <idx_t ICEBERG_VERSION>
idx_t ProduceManifests(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                       vector<IcebergManifest> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);
	auto partition_spec_id =
	    FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("partition_spec_id").GetPrimaryIndex()]);

	int32_t *content = nullptr;
	int64_t *sequence_number = nullptr;
	int64_t *added_rows_count = nullptr;
	int64_t *existing_rows_count = nullptr;

	if (ICEBERG_VERSION > 1) {
		//! 'content'
		content = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("content").GetPrimaryIndex()]);
		//! 'sequence_number'
		sequence_number = FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("sequence_number").GetPrimaryIndex()]);
		//! 'added_rows_count'
		added_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("added_rows_count").GetPrimaryIndex()]);
		//! 'existing_rows_count'
		existing_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("existing_rows_count").GetPrimaryIndex()]);
	}

	//! 'partitions'
	list_entry_t *field_summary = nullptr;
	optional_ptr<Vector> contains_null = nullptr;
	optional_ptr<Vector> contains_nan = nullptr;
	optional_ptr<Vector> lower_bound = nullptr;
	optional_ptr<Vector> upper_bound = nullptr;

	bool *contains_null_data = nullptr;
	bool *contains_nan_data = nullptr;

	auto partitions_it = name_to_vec.find("partitions");
	if (partitions_it != name_to_vec.end()) {
		auto &partitions = chunk.data[name_to_vec.at("partitions").GetPrimaryIndex()];

		auto &field_summary_vec = ListVector::GetEntry(partitions);
		field_summary = FlatVector::GetData<list_entry_t>(partitions);
		auto &child_vectors = StructVector::GetEntries(field_summary_vec);
		auto &child_types = StructType::GetChildTypes(ListType::GetChildType(partitions.GetType()));
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &kv = child_types[i];
			auto &name = kv.first;

			if (StringUtil::CIEquals(name, "contains_null")) {
				contains_null = child_vectors[i].get();
				contains_null_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "contains_nan")) {
				contains_nan = child_vectors[i].get();
				contains_nan_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "lower_bound")) {
				lower_bound = child_vectors[i].get();
			} else if (StringUtil::CIEquals(name, "upper_bound")) {
				upper_bound = child_vectors[i].get();
			}
		}
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[index].GetString();
		manifest.partition_spec_id = partition_spec_id[index];
		manifest.sequence_number = 0;

		if (ICEBERG_VERSION > 1) {
			manifest.content = IcebergManifestContentType(content[index]);
			manifest.sequence_number = sequence_number[index];
			manifest.added_rows_count = added_rows_count[index];
			manifest.existing_rows_count = existing_rows_count[index];
		} else {
			manifest.content = IcebergManifestContentType::DATA;
			manifest.sequence_number = 0;
			manifest.added_rows_count = 0;
			manifest.existing_rows_count = 0;
		}

		if (field_summary) {
			auto list_entry = field_summary[index];
			for (idx_t j = 0; j < list_entry.length; j++) {
				FieldSummary summary;
				auto list_idx = list_entry.offset + j;
				if (contains_null && FlatVector::Validity(*contains_null).RowIsValid(list_idx)) {
					summary.contains_null = contains_null_data[list_idx];
				}
				if (contains_nan && FlatVector::Validity(*contains_nan).RowIsValid(list_idx)) {
					summary.contains_nan = contains_nan_data[list_idx];
				}
				if (lower_bound) {
					summary.lower_bound = lower_bound->GetValue(list_idx);
				}
				if (upper_bound) {
					summary.upper_bound = upper_bound->GetValue(list_idx);
				}
				manifest.field_summary.push_back(summary);
			}
		}

		result.push_back(manifest);
	}
	return count;
}

idx_t IcebergManifestV1::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                                        vector<entry_type> &result) {
	return ProduceManifests<IcebergManifestV1::FORMAT_VERSION>(chunk, offset, count, input, result);
}

bool IcebergManifestV1::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!name_to_vec.count("manifest_path")) {
		return false;
	}
	if (!name_to_vec.count("partition_spec_id")) {
		return false;
	}
	return true;
}

void IcebergManifestV1::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                            case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

idx_t IcebergManifestV2::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                                        vector<entry_type> &result) {
	return ProduceManifests<IcebergManifestV2::FORMAT_VERSION>(chunk, offset, count, input, result);
}

bool IcebergManifestV2::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!IcebergManifestV1::VerifySchema(name_to_vec)) {
		return false;
	}
	if (!name_to_vec.count("content")) {
		return false;
	}
	return true;
}

void IcebergManifestV2::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                            case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

//! Iceberg Manifest Entry scan routines

static void EntryNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                             case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	auto lname = StringUtil::Lower(name);
	if (lname != "data_file") {
		name_to_vec[lname] = ColumnIndex(column_id);
		return;
	}

	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The 'data_file' of the manifest should be a STRUCT");
	}
	auto &children = StructType::GetChildTypes(type);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		auto child_name = StringUtil::Lower(child.first);

		name_to_vec[child_name] = ColumnIndex(column_id, {ColumnIndex(child_idx)});
	}
}

static unordered_map<int32_t, Value> GetBounds(Vector &bounds, idx_t index) {
	auto &bounds_child = ListVector::GetEntry(bounds);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(bounds_child)[0]);
	auto &values = *StructVector::GetEntries(bounds_child)[1];
	auto bounds_list = FlatVector::GetData<list_entry_t>(bounds);

	unordered_map<int32_t, Value> parsed_bounds;

	auto &validity = FlatVector::Validity(bounds);
	if (!validity.RowIsValid(index)) {
		return parsed_bounds;
	}

	auto list_entry = bounds_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_bounds[keys[list_idx]] = values.GetValue(list_idx);
	}
	return parsed_bounds;
}

static unordered_map<int32_t, int64_t> GetCounts(Vector &counts, idx_t index) {
	auto &counts_child = ListVector::GetEntry(counts);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(counts_child)[0]);
	auto values = FlatVector::GetData<int64_t>(*StructVector::GetEntries(counts_child)[1]);
	auto counts_list = FlatVector::GetData<list_entry_t>(counts);

	unordered_map<int32_t, int64_t> parsed_counts;

	auto &validity = FlatVector::Validity(counts);
	if (!validity.RowIsValid(index)) {
		return parsed_counts;
	}

	auto list_entry = counts_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_counts[keys[list_idx]] = values[list_idx];
	}
	return parsed_counts;
}

static vector<int32_t> GetEqualityIds(Vector &equality_ids, idx_t index) {
	vector<int32_t> result;

	if (!FlatVector::Validity(equality_ids).RowIsValid(index)) {
		return result;
	}
	auto &equality_ids_child = ListVector::GetEntry(equality_ids);
	auto equality_ids_data = FlatVector::GetData<int32_t>(equality_ids_child);
	auto equality_ids_list = FlatVector::GetData<list_entry_t>(equality_ids);
	auto list_entry = equality_ids_list[index];

	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		result.push_back(equality_ids_data[list_idx]);
	}

	return result;
}

template <idx_t ICEBERG_VERSION>
idx_t ProduceManifestEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                             vector<IcebergManifestEntry> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto status = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex() == data_file_idx);
	if (ICEBERG_VERSION > 1) {
		D_ASSERT(name_to_vec.at("content").GetPrimaryIndex() == data_file_idx);
	}
	optional_ptr<Vector> equality_ids;
	optional_ptr<Vector> sequence_number;
	int32_t *content;

	auto partition_idx = name_to_vec.at("partition");
	if (ICEBERG_VERSION > 1) {
		auto equality_ids_it = name_to_vec.find("equality_ids");
		if (equality_ids_it != name_to_vec.end()) {
			equality_ids = *child_entries[equality_ids_it->second.GetChildIndex(0).GetPrimaryIndex()];
		}
		auto sequence_number_it = name_to_vec.find("sequence_number");
		if (sequence_number_it != name_to_vec.end()) {
			sequence_number = chunk.data[sequence_number_it->second.GetPrimaryIndex()];
		}
		content =
		    FlatVector::GetData<int32_t>(*child_entries[name_to_vec.at("content").GetChildIndex(0).GetPrimaryIndex()]);
	}

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format =
	    FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count =
	    FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);
	auto file_size_in_bytes = FlatVector::GetData<int64_t>(
	    *child_entries[name_to_vec.at("file_size_in_bytes").GetChildIndex(0).GetPrimaryIndex()]);
	optional_ptr<Vector> lower_bounds;
	optional_ptr<Vector> upper_bounds;
	optional_ptr<Vector> value_counts;
	optional_ptr<Vector> null_value_counts;
	optional_ptr<Vector> nan_value_counts;

	auto lower_bounds_it = name_to_vec.find("lower_bounds");
	if (lower_bounds_it != name_to_vec.end()) {
		lower_bounds = *child_entries[lower_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto upper_bounds_it = name_to_vec.find("upper_bounds");
	if (upper_bounds_it != name_to_vec.end()) {
		upper_bounds = *child_entries[upper_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto value_counts_it = name_to_vec.find("value_counts");
	if (value_counts_it != name_to_vec.end()) {
		value_counts = *child_entries[value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto null_value_counts_it = name_to_vec.find("null_value_counts");
	if (null_value_counts_it != name_to_vec.end()) {
		null_value_counts = *child_entries[null_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto nan_value_counts_it = name_to_vec.find("nan_value_counts");
	if (nan_value_counts_it != name_to_vec.end()) {
		nan_value_counts = *child_entries[nan_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto &partition_vec = child_entries[partition_idx.GetChildIndex(0).GetPrimaryIndex()];

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		if (input.skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}

		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];
		entry.file_size_in_bytes = file_size_in_bytes[index];

		if (lower_bounds && upper_bounds) {
			entry.lower_bounds = GetBounds(*lower_bounds, index);
			entry.upper_bounds = GetBounds(*upper_bounds, index);
		}
		if (value_counts) {
			entry.value_counts = GetCounts(*value_counts, index);
		}
		if (null_value_counts) {
			entry.null_value_counts = GetCounts(*null_value_counts, index);
		}
		if (nan_value_counts) {
			entry.nan_value_counts = GetCounts(*nan_value_counts, index);
		}

		if (ICEBERG_VERSION > 1) {
			entry.content = (IcebergManifestEntryContentType)content[index];
			if (equality_ids) {
				entry.equality_ids = GetEqualityIds(*equality_ids, index);
			}

			if (sequence_number) {
				auto sequence_numbers = FlatVector::GetData<int64_t>(*sequence_number);
				if (FlatVector::Validity(*sequence_number).RowIsValid(index)) {
					entry.sequence_number = sequence_numbers[index];
				} else {
					//! Value should only be NULL for ADDED manifest entries, to support inheritance
					D_ASSERT(entry.status == IcebergManifestEntryStatusType::ADDED);
					entry.sequence_number = input.sequence_number;
				}
			} else {
				//! Default to sequence number 0
				//! (The 'manifest_file' should also have defaulted to 0)
				D_ASSERT(input.sequence_number == 0);
				entry.sequence_number = 0;
			}
		} else {
			entry.sequence_number = input.sequence_number;
			entry.content = IcebergManifestEntryContentType::DATA;
		}

		entry.partition_spec_id = input.partition_spec_id;
		entry.partition = partition_vec->GetValue(index);
		produced++;
		result.push_back(entry);
	}
	return produced;
}

idx_t IcebergManifestEntryV1::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count,
                                             const ManifestReaderInput &input, vector<entry_type> &result) {
	return ProduceManifestEntries<IcebergManifestEntryV1::FORMAT_VERSION>(chunk, offset, count, input, result);
}

bool IcebergManifestEntryV1::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!name_to_vec.count("status")) {
		return false;
	}
	if (!name_to_vec.count("file_path")) {
		return false;
	}
	if (!name_to_vec.count("file_format")) {
		return false;
	}
	if (!name_to_vec.count("record_count")) {
		return false;
	}
	return true;
}

void IcebergManifestEntryV1::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                                 case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

idx_t IcebergManifestEntryV2::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count,
                                             const ManifestReaderInput &input, vector<entry_type> &result) {
	return ProduceManifestEntries<IcebergManifestEntryV2::FORMAT_VERSION>(chunk, offset, count, input, result);
}

bool IcebergManifestEntryV2::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!IcebergManifestEntryV1::VerifySchema(name_to_vec)) {
		return false;
	}
	if (!name_to_vec.count("content")) {
		return false;
	}
	return true;
}

void IcebergManifestEntryV2::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                                 case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

AvroScan::AvroScan(const string &scan_name, ClientContext &context, const string &path) : context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionHelper::AutoLoadExtension(instance, "avro");

	auto &avro_scan_entry = ExtensionUtil::GetTableFunction(instance, "read_avro");
	avro_scan = avro_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = scan_name;
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	bind_data = avro_scan->bind(context, bind_input, return_types, return_names);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
	local_state = avro_scan->init_local(execution_context, input, global_state.get());
}

bool AvroScan::GetNext(DataChunk &result) {
	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	avro_scan->function(context, function_input, result);

	idx_t count = result.size();
	for (auto &vec : result.data) {
		vec.Flatten(count);
	}
	if (count == 0) {
		finished = true;
		return false;
	}
	return true;
}

void AvroScan::InitializeChunk(DataChunk &chunk) {
	chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
}

bool AvroScan::Finished() const {
	return finished;
}

} // namespace duckdb
