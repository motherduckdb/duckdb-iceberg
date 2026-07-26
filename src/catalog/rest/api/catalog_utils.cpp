#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"

namespace duckdb {

JSONValue ICUtils::GetErrorMessage(const string &api_result, unique_ptr<JSONDocument> &out_doc) {
	out_doc = JSONDocument::Parse(api_result.c_str(), api_result.size());
	auto root = out_doc->GetRoot();
	auto error = root.GetMember("error");

	if (!error.IsValid()) {
		return JSONValue();
	}
	auto message = error.GetMember("message");
	auto type = error.GetMember("type");
	auto code = error.GetMember("code");
	if (message.IsValid() && type.IsValid() && code.IsValid()) {
		return root;
	}
	return JSONValue();
}

unique_ptr<JSONDocument> ICUtils::APIResultToDoc(const string &api_result) {
	auto doc = JSONDocument::Parse(api_result.c_str(), api_result.size());
	auto root = doc->GetRoot();
	auto error = root.GetMember("error");
	if (error.IsValid()) {
		try {
			auto message = error.GetMember("message");
			throw InvalidInputException(message.IsString() ? message.GetString() : "No message available");
		} catch (InvalidConfigurationException &e) {
			// keep going, we will throw the whole api result as an error message
			throw InvalidConfigurationException(api_result);
		}
		throw InvalidConfigurationException("Could not parse api_result");
	}
	return doc;
}

} // namespace duckdb
