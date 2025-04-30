#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_manifest.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_types.hpp"
#include "manifest_reader.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

template <class OP>
static void ReadManifestEntries(ClientContext &context, const vector<IcebergManifest> &manifests,
                                bool allow_moved_paths, FileSystem &fs, const string &iceberg_path,
                                vector<IcebergTableEntry> &result) {
	for (auto &manifest : manifests) {
		auto manifest_entry_full_path = allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto manifest_paths = ScanAvroMetadata<OP>("IcebergManifest", context, manifest_entry_full_path);
		result.push_back({std::move(manifest), std::move(manifest_paths)});
	}
}

IcebergTable IcebergTable::Load(const string &iceberg_path, IcebergSnapshot &snapshot, ClientContext &context,
                                const IcebergOptions &options) {
	IcebergTable ret;
	ret.path = iceberg_path;
	ret.snapshot = snapshot;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	vector<IcebergManifest> manifests;
	if (snapshot.iceberg_format_version == 1) {
		manifests = ScanAvroMetadata<IcebergManifestV1>("IcebergManifestList", context, manifest_list_full_path);
		ReadManifestEntries<IcebergManifestEntryV1>(context, manifests, options.allow_moved_paths, fs, iceberg_path,
		                                            ret.entries);
	} else if (snapshot.iceberg_format_version == 2) {
		manifests = ScanAvroMetadata<IcebergManifestV2>("IcebergManifestList", context, manifest_list_full_path);
		ReadManifestEntries<IcebergManifestEntryV2>(context, manifests, options.allow_moved_paths, fs, iceberg_path,
		                                            ret.entries);
	} else {
		throw InvalidInputException("iceberg_format_version %d not handled", snapshot.iceberg_format_version);
	}

	return ret;
}

static void ParseFieldMappings(yyjson_val *obj, case_insensitive_map_t<idx_t> &name_to_mapping_index,
                               vector<IcebergFieldMapping> &mappings, idx_t &mapping_index) {
	case_insensitive_map_t<IcebergFieldMapping> result;
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(obj, idx, max, val) {
		auto names = yyjson_obj_get(val, "names");
		auto field_id = yyjson_obj_get(val, "field-id");
		auto fields = yyjson_obj_get(val, "fields");

		//! Create a new mapping entry
		mappings.push_back(IcebergFieldMapping());
		auto &mapping = mappings.back();

		if (!names) {
			throw InvalidInputException("Corrupt metadata.json file, field-mapping is missing names!");
		}

		//! Map every entry in the 'names' list to the entry we created above
		size_t names_idx, names_max;
		yyjson_val *names_val;
		yyjson_arr_foreach(names, names_idx, names_max, names_val) {
			name_to_mapping_index[yyjson_get_str(names_val)] = mapping_index;
		}
		mapping_index++;

		if (field_id) {
			mapping.field_id = yyjson_get_sint(field_id);
		}
		//! Create mappings for the the nested fields
		if (fields) {
			ParseFieldMappings(fields, mapping.field_mapping_indexes, mappings, mapping_index);
		}
	}
}

unique_ptr<IcebergMetadata> IcebergMetadata::Parse(const string &path, FileSystem &fs,
                                                   const string &metadata_compression_codec) {
	auto metadata = unique_ptr<IcebergMetadata>(new IcebergMetadata);

	if (metadata_compression_codec == "gzip" || StringUtil::EndsWith(path, "gz.metadata.json")) {
		metadata->document = IcebergUtils::GzFileToString(path, fs);
	} else {
		metadata->document = IcebergUtils::FileToString(path, fs);
	}
	metadata->doc = yyjson_read(metadata->document.c_str(), metadata->document.size(), 0);
	if (metadata->doc == nullptr) {
		throw InvalidInputException("Fails to parse iceberg metadata from %s", path);
	}

	auto &info = *metadata;
	auto root = yyjson_doc_get_root(info.doc);
	info.iceberg_version = IcebergUtils::TryGetNumFromObject(root, "format-version");
	info.snapshots = yyjson_obj_get(root, "snapshots");

	// Multiple schemas can be present in the json metadata 'schemas' list
	if (yyjson_obj_getn(root, "current-schema-id", string("current-schema-id").size())) {
		size_t idx, max;
		yyjson_val *schema;
		info.schema_id = IcebergUtils::TryGetNumFromObject(root, "current-schema-id");
		auto schemas = yyjson_obj_get(root, "schemas");
		yyjson_arr_foreach(schemas, idx, max, schema) {
			info.schemas.push_back(schema);
		}
	} else {
		auto schema = yyjson_obj_get(root, "schema");
		if (!schema) {
			throw InvalidConfigurationException("Neither a valid schema or schemas field was found");
		}
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		info.schemas.push_back(schema);
		info.schema_id = found_schema_id;
	}

	auto properties = yyjson_obj_get(root, "properties");
	if (properties) {
		auto name_mapping_defaults_p = yyjson_obj_get(properties, "schema.name-mapping.default");
		if (name_mapping_defaults_p) {
			string name_mapping_default = yyjson_get_str(name_mapping_defaults_p);
			auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
			    yyjson_read(name_mapping_default.c_str(), name_mapping_default.size(), 0));
			if (doc == nullptr) {
				throw InvalidInputException(
				    "Fails to parse iceberg metadata 'schema.name-mapping.default' property from %s", path);
			}
			auto root = yyjson_doc_get_root(doc.get());
			idx_t mapping_index = 0;
			ParseFieldMappings(root, metadata->root_field_mapping.field_mapping_indexes, metadata->mappings,
			                   mapping_index);
		}
	}
	return metadata;
}

IcebergSnapshot IcebergSnapshot::GetLatestSnapshot(IcebergMetadata &info, const IcebergOptions &options) {
	auto latest_snapshot = FindLatestSnapshotInternal(info.snapshots);
	return ParseSnapShot(latest_snapshot, info, options);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotById(IcebergMetadata &info, idx_t snapshot_id,
                                                 const IcebergOptions &options) {
	auto snapshot = FindSnapshotByIdInternal(info.snapshots, snapshot_id);

	if (!snapshot) {
		throw InvalidConfigurationException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot, info, options);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotByTimestamp(IcebergMetadata &info, timestamp_t timestamp,
                                                        const IcebergOptions &options) {
	auto snapshot = FindSnapshotByIdTimestampInternal(info.snapshots, timestamp);

	if (!snapshot) {
		throw InvalidConfigurationException("Could not find latest snapshots for timestamp " +
		                                    Timestamp::ToString(timestamp));
	}

	return ParseSnapShot(snapshot, info, options);
}

// Function to generate a metadata file url from version and format string
// default format is "v%s%s.metadata.json" -> v00###-xxxxxxxxx-.gz.metadata.json"
static string GenerateMetaDataUrl(FileSystem &fs, const string &meta_path, string &table_version,
                                  const IcebergOptions &options) {
	// TODO: Need to URL Encode table_version
	string compression_suffix = "";
	string url;
	if (options.metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}
	for (auto try_format : StringUtil::Split(options.version_name_format, ',')) {
		url = fs.JoinPath(meta_path, StringUtil::Format(try_format, table_version, compression_suffix));
		if (fs.FileExists(url)) {
			return url;
		}
	}

	throw InvalidConfigurationException(
	    "Iceberg metadata file not found for table version '%s' using '%s' compression and format(s): '%s'",
	    table_version, options.metadata_compression_codec, options.version_name_format);
}

string IcebergSnapshot::GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
                                        const IcebergOptions &options) {
	string version_hint;
	string meta_path = fs.JoinPath(path, "metadata");

	auto &table_version = options.table_version;

	if (StringUtil::EndsWith(path, ".json")) {
		// We've been given a real metadata path. Nothing else to do.
		return path;
	}
	if (StringUtil::EndsWith(table_version, ".text") || StringUtil::EndsWith(table_version, ".txt")) {
		// We were given a hint filename
		version_hint = GetTableVersionFromHint(meta_path, fs, table_version);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (table_version != UNKNOWN_TABLE_VERSION) {
		// We were given an explicit version number
		version_hint = table_version;
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (fs.FileExists(fs.JoinPath(meta_path, DEFAULT_VERSION_HINT_FILE))) {
		// We're guessing, but a version-hint.text exists so we'll use that
		version_hint = GetTableVersionFromHint(meta_path, fs, DEFAULT_VERSION_HINT_FILE);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, options);
	}
	if (!UnsafeVersionGuessingEnabled(context)) {
		// Make sure we're allowed to guess versions
		throw InvalidConfigurationException(
		    "Failed to read iceberg table. No version was provided and no version-hint could be found, globbing the "
		    "filesystem to locate the latest version is disabled by default as this is considered unsafe and could "
		    "result in reading uncommitted data. To enable this use 'SET %s = true;'",
		    VERSION_GUESSING_CONFIG_VARIABLE);
	}

	// We are allowed to guess to guess from file paths
	return GuessTableVersion(meta_path, fs, options);
}

IcebergSnapshot IcebergSnapshot::ParseSnapShot(yyjson_val *snapshot, IcebergMetadata &metadata,
                                               const IcebergOptions &options) {
	IcebergSnapshot ret;
	if (snapshot) {
		auto snapshot_tag = yyjson_get_type(snapshot);
		if (snapshot_tag != YYJSON_TYPE_OBJ) {
			throw InvalidConfigurationException("Invalid snapshot field found parsing iceberg metadata.json");
		}
		ret.metadata_compression_codec = options.metadata_compression_codec;
		if (metadata.iceberg_version == 1) {
			ret.sequence_number = 0;
		} else if (metadata.iceberg_version == 2) {
			ret.sequence_number = IcebergUtils::TryGetNumFromObject(snapshot, "sequence-number");
		}
		ret.snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");
		ret.timestamp_ms = Timestamp::FromEpochMs(IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms"));
		auto schema_id = yyjson_obj_get(snapshot, "schema-id");
		if (options.snapshot_source == SnapshotSource::LATEST) {
			ret.schema_id = metadata.schema_id;
		} else if (schema_id && yyjson_get_type(schema_id) == YYJSON_TYPE_NUM) {
			ret.schema_id = yyjson_get_uint(schema_id);
		} else {
			//! 'schema-id' is optional in the V1 iceberg format.
			D_ASSERT(metadata.iceberg_version == 1);
			ret.schema_id = metadata.schema_id;
		}
		ret.manifest_list = IcebergUtils::TryGetStrFromObject(snapshot, "manifest-list");
	} else {
		ret.snapshot_id = DConstants::INVALID_INDEX;
		ret.schema_id = metadata.schema_id;
	}

	ret.iceberg_format_version = metadata.iceberg_version;
	if (options.infer_schema) {
		ret.schema = ParseSchema(metadata.schemas, ret.schema_id);
	}
	return ret;
}

string IcebergSnapshot::GetTableVersionFromHint(const string &meta_path, FileSystem &fs,
                                                string version_file = DEFAULT_VERSION_HINT_FILE) {
	auto version_file_path = fs.JoinPath(meta_path, version_file);
	auto version_file_content = IcebergUtils::FileToString(version_file_path, fs);

	try {
		return version_file_content;
	} catch (std::invalid_argument &e) {
		throw InvalidConfigurationException("Iceberg version hint file contains invalid value");
	} catch (std::out_of_range &e) {
		throw InvalidConfigurationException("Iceberg version hint file contains invalid value");
	}
}

bool IcebergSnapshot::UnsafeVersionGuessingEnabled(ClientContext &context) {
	Value result;
	(void)context.TryGetCurrentSetting(VERSION_GUESSING_CONFIG_VARIABLE, result);
	return !result.IsNull() && result.GetValue<bool>();
}

string IcebergSnapshot::GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options) {
	string selected_metadata;
	string version_pattern = "*"; // TODO: Different "table_version" strings could customize this
	string compression_suffix = "";

	auto &metadata_compression_codec = options.metadata_compression_codec;
	auto &version_format = options.version_name_format;

	if (metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}

	for (auto try_format : StringUtil::Split(version_format, ',')) {
		auto glob_pattern = StringUtil::Format(try_format, version_pattern, compression_suffix);

		auto found_versions = fs.Glob(fs.JoinPath(meta_path, glob_pattern));
		if (found_versions.size() > 0) {
			selected_metadata = PickTableVersion(found_versions, version_pattern, glob_pattern);
			if (!selected_metadata.empty()) { // Found one
				return selected_metadata;
			}
		}
	}

	throw InvalidConfigurationException(
	    "Could not guess Iceberg table version using '%s' compression and format(s): '%s'", metadata_compression_codec,
	    version_format);
}

string IcebergSnapshot::PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern, string &glob) {
	// TODO: Different "table_version" strings could customize this
	// For now: just sort the versions and take the largest
	if (!found_metadata.empty()) {
		std::sort(found_metadata.begin(), found_metadata.end(),
		          [](const OpenFileInfo &a, const OpenFileInfo &b) { return a.path < b.path; });
		return found_metadata.back().path;
	} else {
		return string();
	}
}

yyjson_val *IcebergSnapshot::FindLatestSnapshotInternal(yyjson_val *snapshots) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_timestamp = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto timestamp = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");
		if (timestamp >= max_timestamp) {
			max_timestamp = timestamp;
			max_snapshot = snapshot;
		}
	}

	return max_snapshot;
}

yyjson_val *IcebergSnapshot::FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id) {
	size_t idx, max;
	yyjson_val *snapshot;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");

		if (snapshot_id == target_id) {
			return snapshot;
		}
	}

	return nullptr;
}

yyjson_val *IcebergSnapshot::IcebergSnapshot::FindSnapshotByIdTimestampInternal(yyjson_val *snapshots,
                                                                                timestamp_t timestamp) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {
		auto curr_millis = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");

		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}

	return max_snapshot;
}

} // namespace duckdb
