#include "iceberg_options.hpp"

namespace duckdb {

IcebergSnapshotLookup IcebergSnapshotLookup::FromTimestamp(timestamp_ms_t snapshot_timestamp) {
	//! A snapshot can only ever be created in the past, so a timestamp beyond the current time can not identify a
	//! meaningful point in the history of the table - it would silently resolve to the latest snapshot.
	//! Note that a timestamp merely *after the last snapshot* is perfectly valid, it resolves to that last snapshot.
	//! The current time is converted through the exact same cast that produced 'snapshot_timestamp'. That conversion
	//! rounds to milliseconds, so truncating here instead would make a 'now()' that rounds up look like it lies one
	//! millisecond in the future. Since the cast is monotonic and this runs after the value was produced, the
	//! comparison below can not report a false positive.
	auto now = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp())
	               .DefaultCastAs(LogicalType::TIMESTAMP_MS)
	               .GetValue<timestamp_ms_t>();
	if (snapshot_timestamp > now) {
		throw InvalidInputException(
		    "Can not time travel to '%s', it lies in the future (the current timestamp is '%s')",
		    Value::TIMESTAMPMS(snapshot_timestamp).ToString(), Value::TIMESTAMPMS(now).ToString());
	}
	return IcebergSnapshotLookup(SnapshotFromTimestamp(snapshot_timestamp));
}

IcebergSnapshotLookup IcebergSnapshotLookup::FromAtClause(optional_ptr<BoundAtClause> at) {
	if (!at) {
		return FromLatest();
	}

	auto &unit = at->Unit();
	auto &value = at->GetValue();

	if (value.IsNull()) {
		throw InvalidInputException("NULL values can not be used as the 'unit' of a time travel clause");
	}
	if (unit == "version") {
		return FromSnapshotId(value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>());
	} else if (unit == "timestamp") {
		return FromTimestamp(value.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>());
	} else {
		throw InvalidInputException(
		    "Unit '%s' for time travel is not valid, supported options are 'version' and 'timestamp'",
		    unit.GetIdentifierName());
	}
}

} // namespace duckdb
