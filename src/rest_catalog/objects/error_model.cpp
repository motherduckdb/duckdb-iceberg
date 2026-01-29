
#include "rest_catalog/objects/error_model.hpp"

#include "catalog_utils.hpp"
#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ErrorModel::ErrorModel() {
}

ErrorModel ErrorModel::FromJSON(yyjson_val *obj) {
	ErrorModel res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ErrorModel::TryFromJSON(yyjson_val *obj) {
	string error;
	auto message_val = yyjson_obj_get(obj, "message");
	if (!message_val) {
		return "ErrorModel required property 'message' is missing";
	} else {
		if (yyjson_is_str(message_val)) {
			message = yyjson_get_str(message_val);
		} else {
			return StringUtil::Format("ErrorModel property 'message' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(message_val));
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "ErrorModel required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("ErrorModel property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
	}
	auto code_val = yyjson_obj_get(obj, "code");
	if (!code_val) {
		return "ErrorModel required property 'code' is missing";
	} else {
		if (yyjson_is_int(code_val)) {
			code = yyjson_get_int(code_val);
		} else {
			return StringUtil::Format("ErrorModel property 'code' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(code_val));
		}
	}
	auto stack_val = yyjson_obj_get(obj, "stack");
	if (stack_val) {
		if (yyjson_is_arr(stack_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stack_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("ErrorModel property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				stack.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ErrorModel property 'stack' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(stack_val));
		}
	}
	return string();
}

string ErrorModel::ToString() const {
	unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc(yyjson_mut_doc_new(nullptr));

	auto error_obj = yyjson_mut_obj(doc.get());
	yyjson_mut_obj_add_str(doc.get(), error_obj, "message", message.c_str());
	yyjson_mut_obj_add_str(doc.get(), error_obj, "type", type.c_str());
	yyjson_mut_obj_add_int(doc.get(), error_obj, "code", code);

	if (!stack.empty()) {
		auto stack_arr = yyjson_mut_arr(doc.get());
		for (const auto &s : stack) {
			yyjson_mut_arr_add_str(doc.get(), stack_arr, s.c_str());
		}
		yyjson_mut_obj_add_val(doc.get(), error_obj, "stack", stack_arr);
	}

	auto root = yyjson_mut_obj(doc.get());

	yyjson_mut_obj_add_val(doc.get(), root, "error", error_obj);
	yyjson_mut_doc_set_root(doc.get(), root);

	yyjson_write_err err;
	size_t len;
	const string json_str = yyjson_mut_write_opts(doc.get(), YYJSON_WRITE_PRETTY, nullptr, &len, &err);
	if (err.code != YYJSON_WRITE_SUCCESS) {
		throw InternalException("ErrorModel::ToString() failed to write JSON: %s", err.msg);
	}

	return json_str;
}

} // namespace rest_api_objects
} // namespace duckdb
