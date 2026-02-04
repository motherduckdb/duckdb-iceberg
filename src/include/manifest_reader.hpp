#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

#include "iceberg_options.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

namespace duckdb {

class AvroScan;

class BaseManifestReader {
public:
	BaseManifestReader(AvroScan &scan);
	virtual ~BaseManifestReader();

public:
	void Initialize();
	bool Finished() const;
	virtual void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) = 0;

protected:
	idx_t ScanInternal(idx_t remaining);

protected:
	AvroScan &scan;
	DataChunk chunk;
	unordered_map<int32_t, idx_t> partition_fields;
	unique_ptr<LocalTableFunctionState> local_state;
	idx_t iceberg_version;
	idx_t offset = 0;
	bool finished = true;
};

namespace manifest_list {

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(AvroScan &scan);
	~ManifestListReader() override;

public:
	idx_t Read(idx_t count, vector<IcebergManifestFile> &result);
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestFile> &result);
};

} // namespace manifest_list

namespace manifest_file {

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestFileReader : public BaseManifestReader {
public:
	ManifestFileReader(AvroScan &scan, bool skip_deleted);
	~ManifestFileReader() override;

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateVectorMapping(idx_t i, MultiFileColumnDefinition &column) override;

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace manifest_file

} // namespace duckdb
