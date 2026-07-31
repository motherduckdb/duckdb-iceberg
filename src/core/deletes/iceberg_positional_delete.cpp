#include "core/deletes/iceberg_positional_delete.hpp"

namespace duckdb {

unique_ptr<DeleteFilter> IcebergPositionalDeleteData::ToFilter() const {
	return make_uniq<IcebergPositionalDeleteFilter>(shared_from_this());
}

void IcebergPositionalDeleteData::ToSet(set<idx_t> &out) const {
	out.insert(invalid_rows.begin(), invalid_rows.end());
}

} // namespace duckdb
