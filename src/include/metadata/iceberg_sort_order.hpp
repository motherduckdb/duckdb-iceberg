#pragma once

#include "metadata/iceberg_transform.hpp"
#include "duckdb/common/types/vector.hpp"

#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct IcebergSortOrderField {
public:
	static IcebergSortOrderField ParseFromJson(rest_api_objects::SortField &field);

public:
	int32_t source_id;
	//! "Applied to the source column(s) to produce a partition value"
	IcebergTransform transform;
	//! NOTE: v3 replaces 'source-id' with 'source-ids'
	//! "A source column id or a list of source column ids from the table’s schema"
	string direction;
	//! "Used to identify a sort field and is unique within a sort order spec"
	string null_order;
};

struct IcebergSortOrder {
public:
	static IcebergSortOrder ParseFromJson(rest_api_objects::SortOrder &sort_order_spec);

	bool IsSorted() const;

public:
	int32_t sort_order_id;
	vector<IcebergSortOrderField> fields;
};

} // namespace duckdb
