#pragma once

#include "planning/metadata_io/base_manifest_reader.hpp"

namespace duckdb {

struct IcebergManifestReaderInput {
public:
	IcebergManifestReaderInput(const IcebergManifestMetadata &metadata, const IcebergPartitionSpec &partition_spec)
	    : metadata(metadata), partition_spec(partition_spec) {
	}

public:
	const IcebergManifestMetadata &metadata;
	const IcebergPartitionSpec &partition_spec;
};

namespace manifest_file {

class ManifestReader : public BaseManifestReader {
public:
	ManifestReader(const AvroScan &scan);
	~ManifestReader() override;

public:
	void Read();

public:
	static void ReadChunk(DataChunk &chunk, const map<idx_t, LogicalType> &partition_field_id_to_type,
	                      IcebergManifestReaderInput &reader_input, vector<IcebergManifestEntry> &result);
};

} // namespace manifest_file

} // namespace duckdb
