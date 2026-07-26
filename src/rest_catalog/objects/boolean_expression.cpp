
#include "rest_catalog/objects/boolean_expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

BooleanExpression::BooleanExpression() {
}

BooleanExpression BooleanExpression::FromJSON(JSONValue obj) {
	BooleanExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

BooleanExpression BooleanExpression::Copy() const {
	BooleanExpression res;
	res.value = value;
	return res;
}

string BooleanExpression::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsBoolean(obj)) {
		value = json_utils::GetBoolean(obj);
	} else {
		return StringUtil::Format("BooleanExpression property 'value' is not of type 'boolean', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue BooleanExpression::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateBoolean(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
