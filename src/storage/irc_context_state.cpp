#include "storage/irc_context_state.hpp"

#include "storage/irc_schema_entry.hpp"
#include "storage/irc_catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "catalog_api.hpp"

namespace duckdb {

void IRCContextState::QueryEnd(ClientContext &context) {
	for (auto &it : schemas) {
		auto &schema = it.get();
		if (schema.existence_state.type == SchemaExistenceType::UNKNOWN &&
		    schema.existence_state.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			auto &ic_catalog = schema.catalog.Cast<IRCatalog>();
			auto schema_exists = IRCAPI::VerifySchemaExistence(context, ic_catalog, schema.name);
			if (!schema_exists) {
				throw CatalogException("Namespace by the name of '%s' does not exist in catalog '%s'",
				                       ic_catalog.GetName());
			}
		}
	}
}

void IRCContextState::RegisterSchema(IRCSchemaEntry &schema) {
	schemas.push_back(schema);
}

} // namespace duckdb
