#include "core/expression/iceberg_predicate_stats.hpp"

#include "core/expression/iceberg_value.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "iceberg_logging.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

namespace {

idx_t ValidUtf8PrefixLength(const char *data, idx_t size) {
	idx_t len = size;
	while (len > 0 && !Utf8Proc::IsValid(data, len)) {
		len--;
	}
	return len;
}

//! Sets the repaired bound on 'result', or leaves it unset (and logs) if it cannot be repaired.
void RepairSplitVarcharBound(ClientContext &context, const string_t &blob, SerializeBound bound, const string &name,
                             IcebergPredicateStats &result) {
	bool is_lower = bound == SerializeBound::LOWER_BOUND;
	auto prefix_length = ValidUtf8PrefixLength(blob.GetData(), blob.GetSize());
	if (prefix_length > 0) {
		if (is_lower) {
			result.SetLowerBound(Value(string(blob.GetData(), prefix_length)));
			return;
		}
		// The stored bytes continue past the prefix, so the prefix itself is too small.
		string rounded;
		if (IcebergValue::TruncateAndIncrementString(string(blob.GetData(), blob.GetSize()), rounded, prefix_length)) {
			result.SetUpperBound(Value(std::move(rounded)));
			return;
		}
	}
	DUCKDB_LOG(context, IcebergLogType, "Omitting invalid UTF-8 %s bound for column '%s'", is_lower ? "lower" : "upper",
	           name);
}

} // namespace

void IcebergPredicateStats::SetLowerBound(const Value &new_lower_bound) {
	lower_bound = new_lower_bound;
}

void IcebergPredicateStats::SetUpperBound(const Value &new_upper_bound) {
	upper_bound = new_upper_bound;
}

bool IcebergPredicateStats::BoundsAreNull() const {
	return lower_bound && upper_bound && lower_bound->IsNull() && upper_bound->IsNull();
}

static shared_ptr<BaseStatistics> BuildGeometryStats(const Value &lower_bound, const Value &upper_bound,
                                                     const LogicalType &type) {
	if (lower_bound.IsNull() || upper_bound.IsNull()) {
		return nullptr;
	}
	auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
	auto upper_blob = upper_bound.GetValueUnsafe<string_t>();
	const auto lower_coordinate_card = lower_blob.GetSize() / sizeof(double);
	const auto upper_coordinate_card = upper_blob.GetSize() / sizeof(double);
	if (lower_coordinate_card < 2 || upper_coordinate_card < 2) {
		return nullptr;
	}
	const auto *lo = reinterpret_cast<const double *>(lower_blob.GetData());
	const auto *hi = reinterpret_cast<const double *>(upper_blob.GetData());

	auto stats = make_shared_ptr<BaseStatistics>(GeometryStats::CreateUnknown(type));
	auto &extent = GeometryStats::GetExtent(*stats);
	extent.x_min = lo[0];
	extent.y_min = lo[1];
	extent.x_max = hi[0];
	extent.y_max = hi[1];
	if (lower_coordinate_card >= 3 && upper_coordinate_card >= 3) {
		extent.z_min = lo[2];
		extent.z_max = hi[2];
	}
	if (lower_coordinate_card >= 4 && upper_coordinate_card >= 4) {
		extent.m_min = lo[3];
		extent.m_max = hi[3];
	}
	return stats;
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(ClientContext &context, const Value &lower_bound,
                                                               const Value &upper_bound, const string &name,
                                                               const LogicalType &type) {
	IcebergPredicateStats result;
	if (type.id() == LogicalTypeId::GEOMETRY) {
		result.geometry_stats = BuildGeometryStats(lower_bound, upper_bound, type);
		if (!result.geometry_stats) {
			result.lower_bound.reset();
			result.upper_bound.reset();
		}
		return result;
	}

	if (!lower_bound.IsNull()) {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto blob = lower_bound.GetValueUnsafe<string_t>();
		if (type.id() == LogicalTypeId::VARCHAR && !Utf8Proc::IsValid(blob.GetData(), blob.GetSize())) {
			RepairSplitVarcharBound(context, blob, SerializeBound::LOWER_BOUND, name, result);
		} else {
			auto deserialized = IcebergValue::DeserializeValue(blob, type);
			if (deserialized.HasError()) {
				throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
				                                    deserialized.GetError());
			}
			result.SetLowerBound(deserialized.GetValue());
		}
	}
	if (!upper_bound.IsNull()) {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto blob = upper_bound.GetValueUnsafe<string_t>();
		if (type.id() == LogicalTypeId::VARCHAR && !Utf8Proc::IsValid(blob.GetData(), blob.GetSize())) {
			RepairSplitVarcharBound(context, blob, SerializeBound::UPPER_BOUND, name, result);
		} else {
			auto deserialized = IcebergValue::DeserializeValue(blob, type);
			if (deserialized.HasError()) {
				throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
				                                    deserialized.GetError());
			}
			result.SetUpperBound(deserialized.GetValue());
		}
	}
	return result;
}

} // namespace duckdb
