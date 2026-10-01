#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "core/deletes/iceberg_positional_delete.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <thread>
#include <unordered_set>

using namespace duckdb;

namespace {

bool MatchesRange(DeleteFilter &filter, row_t start, idx_t count, const set<idx_t> &deleted) {
	SelectionVector selection;
	const auto selected = filter.Filter(start, count, selection);
	idx_t expected_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		if (deleted.count(static_cast<idx_t>(start) + i)) {
			continue;
		}
		if (expected_count >= selected || selection.get_index(expected_count) != i) {
			return false;
		}
		++expected_count;
	}
	return selected == expected_count;
}

} // namespace

TEST_CASE("Positional deletes filter sparse, dense and empty ranges", "[iceberg][positional-delete]") {
	auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("deletes.parquet"));
	set<idx_t> deleted;
	auto filter = data->ToFilter();
	REQUIRE(MatchesRange(*filter, 0, 0, deleted));
	REQUIRE(MatchesRange(*filter, 0, STANDARD_VECTOR_SIZE, deleted));

	// Mix isolated positions with a dense range crossing a scan-vector boundary.
	for (idx_t row : {idx_t(0), idx_t(13), idx_t(1024), idx_t(4095), idx_t(8192)}) {
		data->AddRow(row);
		data->AddRow(row);
		deleted.insert(row);
	}
	for (idx_t row = 1800; row < 3600; ++row) {
		data->AddRow(row);
		deleted.insert(row);
	}
	for (row_t start : {row_t(3600), row_t(0), row_t(1800), row_t(4096), row_t(1799), row_t(8192)}) {
		REQUIRE(MatchesRange(*filter, start, STANDARD_VECTOR_SIZE, deleted));
		REQUIRE(MatchesRange(*filter, start, 1, deleted));
		REQUIRE(MatchesRange(*filter, start, 0, deleted));
	}
	REQUIRE(MatchesRange(*filter, 1800, 1800, deleted));
}

TEST_CASE("Positional deletes retain high bits and append to mutation sets", "[iceberg][positional-delete]") {
	const idx_t boundary = idx_t(1) << 32;
	const idx_t largest = static_cast<idx_t>(std::numeric_limits<int64_t>::max());
	auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("first.parquet"));
	auto other = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("second.parquet"));
	const set<idx_t> deleted {0, 7, boundary - 1, boundary, boundary + 7, 2 * boundary + 7, largest};
	for (auto row : deleted) {
		data->AddRow(row);
		data->AddRow(row);
		other->AddRow(row);
	}
	other->AddRow(boundary + 8);
	data->MergeRows(*other);
	auto expected = deleted;
	expected.insert(boundary + 8);
	set<idx_t> actual {42};
	data->ToSet(actual);
	auto expected_set = expected;
	expected_set.insert(42);
	REQUIRE(actual == expected_set);

	set<idx_t> other_set;
	other->ToSet(other_set);
	REQUIRE(other_set == expected);
	data->AddRow(99);
	other_set.clear();
	other->ToSet(other_set);
	REQUIRE(other_set == expected);
	expected.insert(99);
	auto filter = data->ToFilter();
	for (auto start : {idx_t(0), boundary - 2, boundary + 1, 2 * boundary, 3 * boundary, largest - 15}) {
		REQUIRE(MatchesRange(*filter, start, 16, expected));
	}
	REQUIRE(MatchesRange(*filter, boundary - 1, 1, expected));
	REQUIRE(MatchesRange(*filter, boundary + 1, 6, expected));

	auto empty = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("empty.parquet"));
	empty->ToSet(actual);
	REQUIRE(actual == expected_set);
	data->MergeRows(*empty);
	REQUIRE(MatchesRange(*filter, boundary - 2, 16, expected));
}

TEST_CASE("Positional delete insertion resumes after a union replaces containers", "[iceberg][positional-delete]") {
	auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("first.parquet"));
	auto other = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("second.parquet"));
	data->AddRow(0);
	set<idx_t> expected {0};
	for (idx_t row = 1; row < 10000; row += 2) {
		other->AddRow(row);
		expected.insert(row);
	}
	data->MergeRows(*other);
	// Both insertions address the same high 48 bits as the pre-union context.
	data->AddRow(2);
	data->AddRow(10000);
	expected.insert(2);
	expected.insert(10000);
	set<idx_t> actual;
	data->ToSet(actual);
	REQUIRE(actual == expected);
	auto filter = data->ToFilter();
	REQUIRE(MatchesRange(*filter, 0, STANDARD_VECTOR_SIZE, expected));
}

TEST_CASE("Positional delete batches cross the 32-bit boundary", "[iceberg][positional-delete]") {
	const idx_t boundary = idx_t(1) << 32;
	auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("deletes.parquet"));
	set<idx_t> deleted;
	for (idx_t row = boundary - 512; row < boundary + 512; row += 2) {
		data->AddRow(row);
		deleted.insert(row);
	}
	auto filter = data->ToFilter();
	REQUIRE(MatchesRange(*filter, boundary - 513, 1026, deleted));
	REQUIRE(MatchesRange(*filter, boundary - 3, 7, deleted));
	REQUIRE(MatchesRange(*filter, boundary + 1, 511, deleted));
}

#ifndef DUCKDB_NO_THREADS
TEST_CASE("Positional delete filters independently seek shared immutable data", "[iceberg][positional-delete]") {
	const idx_t boundary = idx_t(1) << 32;
	auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("deletes.parquet"));
	set<idx_t> deleted;
	for (idx_t i = 0; i < 8192; i += 3) {
		data->AddRow(i);
		data->AddRow(boundary + i);
		deleted.insert(i);
		deleted.insert(boundary + i);
	}
	std::atomic<bool> matches {true};
	auto check = [&](idx_t high) {
		auto filter = data->ToFilter();
		for (idx_t i = 0; i < 100; ++i) {
			const auto start = high + (i * 997) % 8192;
			if (!MatchesRange(*filter, start, STANDARD_VECTOR_SIZE, deleted)) {
				matches = false;
			}
		}
	};
	std::thread low(check, 0);
	std::thread high(check, boundary);
	low.join();
	high.join();
	REQUIRE(matches.load());
}
#endif

TEST_CASE("Positional delete bitmap microbenchmark", "[.][iceberg][positional-delete-benchmark]") {
	// Explicit opt-in; timings are diagnostic, never pass/fail thresholds.
	const idx_t row_count = 1 << 20;
	const idx_t repetitions = 20;
	for (idx_t stride : {idx_t(1000), idx_t(2)}) {
		auto elapsed_ms = [](std::chrono::steady_clock::time_point start) {
			return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
		};
		auto start = std::chrono::steady_clock::now();
		std::unordered_set<int64_t> reference;
		for (idx_t row = 0; row < row_count; row += stride) {
			reference.insert(row);
		}
		const auto set_build_ms = elapsed_ms(start);
		start = std::chrono::steady_clock::now();
		auto data = make_shared_ptr<IcebergPositionalDeleteData>(IcebergFileIdentity("benchmark.parquet"));
		for (idx_t row = 0; row < row_count; row += stride) {
			data->AddRow(row);
		}
		const auto bitmap_build_ms = elapsed_ms(start);
		auto filter = data->ToFilter();
		SelectionVector selection(STANDARD_VECTOR_SIZE);
		idx_t set_checksum = 0;
		start = std::chrono::steady_clock::now();
		for (idx_t iteration = 0; iteration < repetitions; ++iteration) {
			for (idx_t row = 0; row < row_count; row += STANDARD_VECTOR_SIZE) {
				selection.Initialize(STANDARD_VECTOR_SIZE);
				idx_t selected = 0;
				for (idx_t offset = 0; offset < STANDARD_VECTOR_SIZE; ++offset) {
					if (!reference.count(row + offset)) {
						selection.set_index(selected++, offset);
					}
				}
				set_checksum += selected + selection.get_index(selected - 1);
			}
		}
		const auto set_filter_ms = elapsed_ms(start);
		idx_t bitmap_checksum = 0;
		start = std::chrono::steady_clock::now();
		for (idx_t iteration = 0; iteration < repetitions; ++iteration) {
			for (idx_t row = 0; row < row_count; row += STANDARD_VECTOR_SIZE) {
				const auto selected = filter->Filter(row, STANDARD_VECTOR_SIZE, selection);
				bitmap_checksum += selected + selection.get_index(selected - 1);
			}
		}
		const auto bitmap_filter_ms = elapsed_ms(start);
		REQUIRE(set_checksum == bitmap_checksum);
		std::cout << "stride=" << stride << " rows=" << row_count << " repetitions=" << repetitions
		          << " set_build_ms=" << set_build_ms << " bitmap_build_ms=" << bitmap_build_ms
		          << " set_filter_ms=" << set_filter_ms << " bitmap_filter_ms=" << bitmap_filter_ms
		          << " bitmap_serialized_bytes="
		          << roaring::api::roaring64_bitmap_portable_size_in_bytes(data->invalid_rows.get()) << std::endl;
	}
}
