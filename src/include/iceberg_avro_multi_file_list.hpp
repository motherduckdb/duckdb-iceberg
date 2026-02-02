#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

class IcebergAvroScanInfo : public TableFunctionInfo {
public:
	IcebergAvroScanInfo(bool is_manifest_list, const IcebergTableMetadata &metadata)
	    : is_manifest_list(is_manifest_list), metadata(metadata) {
	}

public:
	bool is_manifest_list;
	const IcebergTableMetadata &metadata;
	//! If 'is_manifest_list' is false, the partition spec ids referenced by the manifests we're scanning
	unordered_set<int32_t> partition_spec_ids;
};

class IcebergAvroMultiFileList : public SimpleMultiFileList {
public:
	IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths)
	    : SimpleMultiFileList(std::move(paths)), info(info) {
	}
	virtual ~IcebergAvroMultiFileList() {
	}

public:
	shared_ptr<IcebergAvroScanInfo> info;
};

} // namespace duckdb
