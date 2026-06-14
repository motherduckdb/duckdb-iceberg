#pragma once

#include "duckdb.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

//! A batch of scanned/cached IcebergManifestEntry items to read
struct ManifestReadBatch {
public:
	ManifestReadBatch() {
	}
	ManifestReadBatch(idx_t manifest_list_entry_idx, idx_t start_index, idx_t end_index)
	    : manifest_list_entry_idx(manifest_list_entry_idx), start_index(start_index), end_index(end_index) {
	}

public:
	idx_t manifest_list_entry_idx;
	idx_t start_index;
	idx_t end_index;
};

struct ManifestEntryReadState {
public:
	void PushBatch(ManifestReadBatch &&batch);
	bool GetBatch(idx_t batch_idx, ManifestReadBatch &result) const;

private:
	//! Lock guarding the batches against concurrent access
	mutable mutex lock;
	vector<ManifestReadBatch> batches;
};

} // namespace duckdb
