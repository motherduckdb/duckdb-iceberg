
#include "rest_catalog/objects/float_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FloatTypeValue::FloatTypeValue() {
}

FloatTypeValue FloatTypeValue::FromJSON(yyjson_val *obj) {
	FloatTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FloatTypeValue FloatTypeValue::Copy() const {
	FloatTypeValue res;
	res.value = value;
	return res;
}

string FloatTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_num(obj)) {
		value = yyjson_get_num(obj);
	} else {
		return StringUtil::Format("FloatTypeValue property 'value' is not of type 'number', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *FloatTypeValue::ToJSON(yyjson_mut_doc *doc) const {
	return yyjson_mut_real(doc, value);
}

} // namespace rest_api_objects
} // namespace duckdb
