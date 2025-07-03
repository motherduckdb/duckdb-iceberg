#include "metadata/iceberg_table_schema.hpp"

#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

shared_ptr<IcebergTableSchema> IcebergTableSchema::ParseSchema(rest_api_objects::Schema &schema) {
	auto res = make_shared_ptr<IcebergTableSchema>();
	res->schema_id = schema.object_1.schema_id;
	for (auto &field : schema.struct_type.fields) {
		res->columns.push_back(IcebergColumnDefinition::ParseStructField(*field));
	}
	return res;
}

void IcebergTableSchema::PopulateSourceIdMap(unordered_map<uint64_t, ColumnIndex> &source_to_column_id,
                                             const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                             optional_ptr<ColumnIndex> parent) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = columns[i];

		ColumnIndex new_index;
		if (parent) {
			auto primary = parent->GetPrimaryIndex();
			auto child_indexes = parent->GetChildIndexes();
			child_indexes.push_back(ColumnIndex(i));
			new_index = ColumnIndex(primary, child_indexes);
		} else {
			new_index = ColumnIndex(i);
		}

		PopulateSourceIdMap(source_to_column_id, column->children, new_index);
		source_to_column_id.emplace(static_cast<uint64_t>(column->id), std::move(new_index));
	}
}

const IcebergColumnDefinition &
IcebergTableSchema::GetFromColumnIndex(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                       const ColumnIndex &column_index, idx_t depth) {
	auto &child_indexes = column_index.GetChildIndexes();
	auto &selected_index = depth ? child_indexes[depth - 1] : column_index;

	auto index = selected_index.GetPrimaryIndex();
	if (index >= columns.size()) {
		throw InvalidConfigurationException("ColumnIndex out of bounds for columns (index %d, 'columns' size: %d)",
		                                    index, columns.size());
	}
	auto &column = columns[index];
	if (depth == child_indexes.size()) {
		return *column;
	}
	if (column->children.empty()) {
		throw InvalidConfigurationException(
		    "Expected column to have children, ColumnIndex has a depth of %d, we reached only %d",
		    column_index.ChildIndexCount(), depth);
	}
	return GetFromColumnIndex(column->children, column_index, depth + 1);
}

} // namespace duckdb
