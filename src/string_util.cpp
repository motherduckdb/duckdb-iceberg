#include "string_util.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

std::vector<uint8_t> HexStringToBytes(const std::string &hex) {
	std::vector<uint8_t> bytes;
	D_ASSERT(hex.size() % 2 == 0);
	bytes.reserve(hex.size() / 2);

	for (size_t i = 0; i < hex.size(); i += 2) {
		uint8_t byte = std::stoi(hex.substr(i, 2), nullptr, 16);
		bytes.push_back(byte);
	}
	return bytes;
}

} // namespace duckdb