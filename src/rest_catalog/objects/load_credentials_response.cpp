
#include "rest_catalog/objects/load_credentials_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LoadCredentialsResponse::LoadCredentialsResponse() {
}

LoadCredentialsResponse LoadCredentialsResponse::FromJSON(JSONValue obj) {
	LoadCredentialsResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadCredentialsResponse LoadCredentialsResponse::Copy() const {
	LoadCredentialsResponse res;
	res.storage_credentials.reserve(storage_credentials.size());
	for (auto &item : storage_credentials) {
		res.storage_credentials.emplace_back(item.Copy());
	}
	return res;
}

string LoadCredentialsResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto storage_credentials_val = obj.GetMember("storage-credentials");
	if (!storage_credentials_val.IsValid()) {
		return "LoadCredentialsResponse required property 'storage-credentials' is missing";
	} else {
		if (storage_credentials_val.IsArray()) {
			storage_credentials_val.IterateArray([&](JSONValue storage_credentials_item_val) {
				if (!error.empty()) {
					return;
				}
				StorageCredential storage_credentials_item;
				error = storage_credentials_item.TryFromJSON(storage_credentials_item_val);
				if (!error.empty()) {
					return;
				}
				storage_credentials.emplace_back(std::move(storage_credentials_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "LoadCredentialsResponse property 'storage_credentials' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(storage_credentials_val).c_str());
		}
	}
	return "";
}

void LoadCredentialsResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: storage-credentials
	auto storage_credentials_arr = writer.CreateArray();
	for (const auto &item : storage_credentials) {
		auto item_val = item.ToJSON(writer);
		storage_credentials_arr.Append(item_val);
	}
	obj.Add("storage-credentials", storage_credentials_arr);
}

JSONMutableValue LoadCredentialsResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
