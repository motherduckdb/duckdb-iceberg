
#include "rest_catalog/objects/base_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

BaseUpdate::BaseUpdate() {
}

BaseUpdate BaseUpdate::FromJSON(JSONValue obj) {
	BaseUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

BaseUpdate BaseUpdate::Copy() const {
	BaseUpdate res;
	res.action = action;
	return res;
}

string BaseUpdate::TryFromJSON(JSONValue obj) {
	string error;
	auto action_val = obj.GetMember("action");
	if (!action_val.IsValid()) {
		return "BaseUpdate required property 'action' is missing";
	} else {
		if (json_utils::IsString(action_val)) {
			action = json_utils::GetString(action_val);
		} else {
			return StringUtil::Format("BaseUpdate property 'action' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(action_val).c_str());
		}
	}
	return "";
}

void BaseUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: action
	obj.AddString("action", action);
}

JSONMutableValue BaseUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
