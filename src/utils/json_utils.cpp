
#include "duckdb/common/string_util.hpp"
#include "utils/json_utils.hpp"

namespace duckdb {

string JSONUtils::JsonDocToString(yyjson_mut_doc *doc) {
	auto root_object = yyjson_mut_doc_get_root(doc);

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
	}
	auto res = string(data);
	free(data);
	return res;
}

string JSONUtils::json_to_string(yyjson_mut_doc *doc, yyjson_write_flag flags) {
	char *json_chars = yyjson_mut_write(doc, flags, NULL);
	string json_str(json_chars);
	free(json_chars);
	return json_str;
}

} // namespace duckdb
