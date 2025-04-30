//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

static string IcebergManifestContentTypeToString(IcebergManifestContentType type) {
	switch (type) {
	case IcebergManifestContentType::DATA:
		return "DATA";
	case IcebergManifestContentType::DELETE:
		return "DELETE";
	default:
		throw InvalidConfigurationException("Invalid Manifest Content Type");
	}
}

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

static string IcebergManifestEntryStatusTypeToString(IcebergManifestEntryStatusType type) {
	switch (type) {
	case IcebergManifestEntryStatusType::EXISTING:
		return "EXISTING";
	case IcebergManifestEntryStatusType::ADDED:
		return "ADDED";
	case IcebergManifestEntryStatusType::DELETED:
		return "DELETED";
	default:
		throw InvalidConfigurationException("Invalid matifest entry type");
	}
}

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

static string IcebergManifestEntryContentTypeToString(IcebergManifestEntryContentType type) {
	switch (type) {
	case IcebergManifestEntryContentType::DATA:
		return "EXISTING";
	case IcebergManifestEntryContentType::POSITION_DELETES:
		return "POSITION_DELETES";
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		return "EQUALITY_DELETES";
	default:
		throw InvalidConfigurationException("Invalid Manifest Entry Content Type");
	}
}

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
public:
	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	sequence_number_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;

public:
	void Print() {
		Printer::Print("  - Manifest = { content: " + IcebergManifestContentTypeToString(content) +
		               ", path: " + manifest_path + "}");
	}

	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR,
		    LogicalType::BIGINT,
		    LogicalType::VARCHAR,
		};
	}

	static vector<string> Names() {
		return {"manifest_path", "manifest_sequence_number", "manifest_content"};
	}
};

//! An entry in a manifest file
struct IcebergManifestEntry {
public:
	IcebergManifestEntryStatusType status;
	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	vector<int32_t> equality_ids;
	int64_t record_count;
	Value partition;
	//! Inherited from the 'manifest_file' if NULL and 'status == EXISTING'
	sequence_number_t sequence_number;
	//! Inherited from the 'manifest_file'
	int32_t partition_spec_id;

public:
	void Print() {
		Printer::Print("    -> ManifestEntry = { type: " + IcebergManifestEntryStatusTypeToString(status) +
		               ", content: " + IcebergManifestEntryContentTypeToString(content) + ", file: " + file_path +
		               ", record_count: " + std::to_string(record_count) + "}");
	}

	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
		};
	}

	static vector<string> Names() {
		return {"status", "content", "file_path", "file_format", "record_count"};
	}
};

struct IcebergTableEntry {
	IcebergManifest manifest;
	vector<IcebergManifestEntry> manifest_entries;

	void Print() {
		manifest.Print();
		for (auto &manifest_entry : manifest_entries) {
			manifest_entry.Print();
		}
	}
};

} // namespace duckdb
