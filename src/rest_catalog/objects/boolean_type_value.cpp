
#include "rest_catalog/objects/boolean_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

BooleanTypeValue BooleanTypeValue::FromJSON(yyjson_val *obj) {
	BooleanTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string BooleanTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_bool(obj)) {
		value = yyjson_get_bool(obj);
	} else {
		return StringUtil::Format("BooleanTypeValue property 'value' is not of type 'boolean', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *BooleanTypeValue::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);

	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
