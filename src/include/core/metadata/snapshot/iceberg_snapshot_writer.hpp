#pragma once

#include "core/metadata/manifest/iceberg_pending_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

class IcebergSnapshotWriter;

//! Metadata files have been written, but publication is still the caller's responsibility.
struct IcebergWrittenSnapshot {
	//! Consume the writer and persist its manifest list. Publication remains with the caller.
	static IcebergWrittenSnapshot Create(IcebergSnapshotWriter writer);

	IcebergSnapshot snapshot;
	vector<IcebergManifestListEntry> manifests;
	int64_t next_row_id;
};

//! Assembles one snapshot in one commit attempt. Pending content remains reusable for retries.
class IcebergSnapshotWriter {
	friend struct IcebergWrittenSnapshot;

public:
	IcebergSnapshotWriter(ClientContext &context, const IcebergTableMetadata &table_metadata, int32_t schema_id,
	                      IcebergSnapshotOperationType operation, sequence_number_t sequence_number,
	                      int64_t next_row_id, vector<string> &created_metadata_files,
	                      optional_ptr<const IcebergSnapshot> parent = nullptr);
	IcebergSnapshotWriter(IcebergSnapshotWriter &&) = default;
	IcebergSnapshotWriter(const IcebergSnapshotWriter &) = delete;
	IcebergSnapshotWriter &operator=(const IcebergSnapshotWriter &) = delete;

	//! Carry forward a written manifest without changing its identity or counting its rows again.
	void AddExistingManifest(IcebergManifestListEntry manifest);
	void WriteManifest(const IcebergPendingManifest &manifest);
	//! Rewrites preserve row lineage and do not count as newly added data.
	IcebergManifestListEntry WriteReplacementManifest(const IcebergManifestMetadata &metadata,
	                                                  vector<IcebergManifestEntry> entries,
	                                                  optional<int64_t> first_row_id);
	void RemoveManifestEntry(const IcebergManifestEntry &entry);
	void SetTotalFilesSize(int64_t total_files_size);

private:
	void WriteManifestFile(IcebergManifestListEntry &manifest);

	ClientContext &context;
	const IcebergTableMetadata &table_metadata;
	DatabaseInstance &db;
	CopyFunction &avro_copy;
	//! Register paths before writing; the caller decides cleanup based on the publication outcome.
	vector<string> &created_metadata_files;
	IcebergSnapshot snapshot;
	IcebergManifestList manifest_list;
	int64_t next_row_id;
};

} // namespace duckdb
