
#include "rest_catalog/objects/commit_view_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CommitViewRequest::CommitViewRequest() {
}

CommitViewRequest CommitViewRequest::FromJSON(JSONValue obj) {
	CommitViewRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitViewRequest CommitViewRequest::Copy() const {
	CommitViewRequest res;
	res.updates.reserve(updates.size());
	for (auto &item : updates) {
		res.updates.emplace_back(item.Copy());
	}
	if (identifier.has_value()) {
		res.identifier.emplace();
		(*res.identifier) = (*identifier).Copy();
	}
	if (requirements.has_value()) {
		res.requirements.emplace();
		(*res.requirements).reserve((*requirements).size());
		for (auto &item : (*requirements)) {
			(*res.requirements).emplace_back(item.Copy());
		}
	}
	return res;
}

string CommitViewRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto updates_val = obj.GetMember("updates");
	if (!updates_val.IsValid()) {
		return "CommitViewRequest required property 'updates' is missing";
	} else {
		if (updates_val.IsArray()) {
			updates_val.IterateArray([&](JSONValue updates_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewUpdate updates_item;
				error = updates_item.TryFromJSON(updates_item_val);
				if (!error.empty()) {
					return;
				}
				updates.emplace_back(std::move(updates_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("CommitViewRequest property 'updates' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(updates_val).c_str());
		}
	}
	auto identifier_val = obj.GetMember("identifier");
	if (identifier_val.IsValid()) {
		TableIdentifier identifier_tmp;
		error = identifier_tmp.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
		identifier = std::move(identifier_tmp);
	}
	auto requirements_val = obj.GetMember("requirements");
	if (requirements_val.IsValid()) {
		vector<ViewRequirement> requirements_tmp;
		if (requirements_val.IsArray()) {
			requirements_val.IterateArray([&](JSONValue requirements_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				ViewRequirement requirements_tmp_item;
				error = requirements_tmp_item.TryFromJSON(requirements_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				requirements_tmp.emplace_back(std::move(requirements_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "CommitViewRequest property 'requirements_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(requirements_val).c_str());
		}
		requirements = std::move(requirements_tmp);
	}
	return "";
}

void CommitViewRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: updates
	auto updates_arr = writer.CreateArray();
	for (const auto &item : updates) {
		auto item_val = item.ToJSON(writer);
		updates_arr.Append(item_val);
	}
	obj.Add("updates", updates_arr);

	// Serialize: identifier
	if (identifier.has_value()) {
		auto &identifier_value = *identifier;
		auto identifier_value_val = identifier_value.ToJSON(writer);
		obj.Add("identifier", identifier_value_val);
	}

	// Serialize: requirements
	if (requirements.has_value()) {
		auto &requirements_value = *requirements;
		auto requirements_value_arr = writer.CreateArray();
		for (const auto &item : requirements_value) {
			auto item_val = item.ToJSON(writer);
			requirements_value_arr.Append(item_val);
		}
		obj.Add("requirements", requirements_value_arr);
	}
}

JSONMutableValue CommitViewRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
