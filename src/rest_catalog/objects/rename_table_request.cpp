
#include "rest_catalog/objects/rename_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RenameTableRequest RenameTableRequest::FromJSON(yyjson_val *obj) {
	RenameTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RenameTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto source_val = yyjson_obj_get(obj, "source");
	if (!source_val) {
		return "RenameTableRequest required property 'source' is missing";
	} else {
		error = source.TryFromJSON(source_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto destination_val = yyjson_obj_get(obj, "destination");
	if (!destination_val) {
		return "RenameTableRequest required property 'destination' is missing";
	} else {
		error = destination.TryFromJSON(destination_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

yyjson_mut_val *RenameTableRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	// Serialize: source
	yyjson_mut_val *source_val = source.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "source", source_val);

	// Serialize: destination
	yyjson_mut_val *destination_val = destination.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "destination", destination_val);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
