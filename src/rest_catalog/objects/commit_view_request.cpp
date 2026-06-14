
#include "rest_catalog/objects/commit_view_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitViewRequest::CommitViewRequest() {
}

CommitViewRequest CommitViewRequest::FromJSON(yyjson_val *obj) {
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
	if (has_identifier) {
		res.identifier = identifier.Copy();
	}
	res.has_identifier = has_identifier;
	if (has_requirements) {
		res.requirements.reserve(requirements.size());
		for (auto &item : requirements) {
			res.requirements.emplace_back(item.Copy());
		}
	}
	res.has_requirements = has_requirements;
	return res;
}

string CommitViewRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto updates_val = yyjson_obj_get(obj, "updates");
	if (!updates_val) {
		return "CommitViewRequest required property 'updates' is missing";
	} else {
		if (yyjson_is_arr(updates_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updates_val, idx, max, val) {
				ViewUpdate tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				updates.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("CommitViewRequest property 'updates' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(updates_val));
		}
	}
	auto identifier_val = yyjson_obj_get(obj, "identifier");
	if (identifier_val && !yyjson_is_null(identifier_val)) {
		has_identifier = true;
		error = identifier.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto requirements_val = yyjson_obj_get(obj, "requirements");
	if (requirements_val && !yyjson_is_null(requirements_val)) {
		has_requirements = true;
		if (yyjson_is_arr(requirements_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(requirements_val, idx, max, val) {
				ViewRequirement tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				requirements.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "CommitViewRequest property 'requirements' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(requirements_val));
		}
	}
	return "";
}

void CommitViewRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: updates
	yyjson_mut_val *updates_arr = yyjson_mut_arr(doc);
	for (const auto &item : updates) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(updates_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "updates", updates_arr);

	// Serialize: identifier
	if (has_identifier) {
		yyjson_mut_val *identifier_val = identifier.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "identifier", identifier_val);
	}

	// Serialize: requirements
	if (has_requirements) {
		yyjson_mut_val *requirements_arr = yyjson_mut_arr(doc);
		for (const auto &item : requirements) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(requirements_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "requirements", requirements_arr);
	}
}

yyjson_mut_val *CommitViewRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
