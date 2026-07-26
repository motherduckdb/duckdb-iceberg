#include "core/metadata/schema/iceberg_field_mapping.hpp"

namespace duckdb {

void IcebergFieldMapping::ParseFieldMappings(JSONValue obj, vector<IcebergFieldMapping> &mappings, idx_t &mapping_index,
                                             idx_t parent_mapping_index) {
	case_insensitive_map_t<IcebergFieldMapping> result;
	obj.IterateArray([&](JSONValue val) {
		auto names = val.GetMember("names");
		auto field_id = val.GetMember("field-id");
		auto fields = val.GetMember("fields");

		//! Create a new mapping entry
		mappings.emplace_back();
		auto &mapping = mappings.back();

		if (!names.IsValid()) {
			throw InvalidInputException("Corrupt metadata.json file, field-mapping is missing names!");
		}
		auto current_mapping_index = mapping_index;

		auto &name_to_mapping_index = mappings[parent_mapping_index].field_mapping_indexes;
		//! Map every entry in the 'names' list to the entry we created above
		names.IterateArray([&](JSONValue name) { name_to_mapping_index[name.GetString()] = current_mapping_index; });
		mapping_index++;

		if (field_id.IsValid()) {
			mapping.field_id = field_id.GetType() == JSONValueType::SIGNED_INTEGER
			                       ? static_cast<int32_t>(field_id.GetSignedInteger())
			                       : static_cast<int32_t>(field_id.GetUnsignedInteger());
		}
		//! Create mappings for the the nested fields
		if (fields.IsValid()) {
			ParseFieldMappings(fields, mappings, mapping_index, current_mapping_index);
		}
	});
}

} // namespace duckdb
