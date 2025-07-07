#pragma once

#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class IRCSchemaEntry;

class IRCContextState : public ClientContextState {
public:
	IRCContextState() {
	}

public:
	void QueryEnd(ClientContext &context) override;
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		QueryEnd(context);
	}

public:
	void RegisterSchema(IRCSchemaEntry &schema);

public:
	vector<reference<IRCSchemaEntry>> schemas;
};

} // namespace duckdb
