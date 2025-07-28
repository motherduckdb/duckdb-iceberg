#pragma once

#include "duckdb.hpp"

namespace duckdb {

class IcebergExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
};

} // namespace duckdb
