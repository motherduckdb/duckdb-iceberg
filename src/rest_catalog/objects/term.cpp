
#include "rest_catalog/objects/term.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Term::Term() {
}

Term Term::FromJSON(yyjson_val *obj) {
	Term res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Term Term::Copy() const {
	Term res;
	if (reference.has_value()) {
		res.reference.emplace();
		(*res.reference) = (*reference).Copy();
	}
	if (transform_term.has_value()) {
		res.transform_term.emplace();
		(*res.transform_term) = (*transform_term).Copy();
	}
	return res;
}

string Term::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		reference.emplace();
		error = reference->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			reference = nullopt;
		}
		transform_term.emplace();
		error = transform_term->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			transform_term = nullopt;
		}
		return "Term failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *Term::ToJSON(yyjson_mut_doc *doc) const {
	if (reference.has_value()) {
		return reference->ToJSON(doc);
	} else if (transform_term.has_value()) {
		return transform_term->ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
