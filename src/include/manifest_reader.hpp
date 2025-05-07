#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

typedef void (*manifest_reader_name_mapping)(idx_t column_id, const LogicalType &type, const string &name,
                                             case_insensitive_map_t<ColumnIndex> &mapping);
typedef bool (*manifest_reader_schema_validation)(const case_insensitive_map_t<ColumnIndex> &mapping);

typedef idx_t (*manifest_reader_manifest_producer)(DataChunk &chunk, idx_t offset, idx_t count,
                                                   const ManifestReaderInput &input, vector<IcebergManifest> &result);
typedef idx_t (*manifest_reader_manifest_entry_producer)(DataChunk &chunk, idx_t offset, idx_t count,
                                                         const ManifestReaderInput &input,
                                                         vector<IcebergManifestEntry> &result);

using manifest_reader_read =
    std::function<idx_t(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input)>;

struct ManifestReaderInput {
public:
	ManifestReaderInput(const case_insensitive_map_t<ColumnIndex> &name_to_vec,
	                    sequence_number_t sequence_number_to_inherit = NumericLimits<sequence_number_t>::Maximum(),
	                    int32_t partition_spec_id = NumericLimits<int32_t>::Maximum(), bool skip_deleted = false);

public:
	const case_insensitive_map_t<ColumnIndex> &name_to_vec;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
	//! The sequence number to inherit when the condition to do so is met
	sequence_number_t sequence_number;
	//! The inherited partition spec id (from the 'manifest_file')
	int32_t partition_spec_id;
};

class ManifestReader {
public:
	ManifestReader(manifest_reader_name_mapping name_mapping, manifest_reader_schema_validation schema_validator);

public:
	void Initialize(unique_ptr<AvroScan> scan_p);
	void SetSequenceNumber(sequence_number_t sequence_number);
	void SetPartitionSpecID(int32_t partition_spec_id);

public:
	bool Finished() const;
	idx_t ReadEntries(idx_t count, manifest_reader_read callback);

private:
	unique_ptr<AvroScan> scan;
	DataChunk chunk;
	idx_t offset = 0;
	bool finished = true;
	case_insensitive_map_t<ColumnIndex> name_to_vec;

	manifest_reader_name_mapping name_mapping = nullptr;
	manifest_reader_schema_validation schema_validation = nullptr;

public:
	bool skip_deleted = false;
	sequence_number_t sequence_number = NumericLimits<sequence_number_t>::Maximum();
	int32_t partition_spec_id = NumericLimits<int32_t>::Maximum();
};

} // namespace duckdb
