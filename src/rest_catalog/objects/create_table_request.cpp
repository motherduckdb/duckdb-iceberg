
#include "rest_catalog/objects/create_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CreateTableRequest::CreateTableRequest() {
}

CreateTableRequest CreateTableRequest::FromJSON(yyjson_val *obj) {
	CreateTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CreateTableRequest CreateTableRequest::Copy() const {
	CreateTableRequest res;
	res.name = name;
	res.schema = schema.Copy();
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	if (partition_spec.has_value()) {
		res.partition_spec.emplace();
		(*res.partition_spec) = (*partition_spec).Copy();
	}
	if (write_order.has_value()) {
		res.write_order.emplace();
		(*res.write_order) = (*write_order).Copy();
	}
	if (stage_create.has_value()) {
		res.stage_create.emplace();
		(*res.stage_create) = (*stage_create);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string CreateTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "CreateTableRequest required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("CreateTableRequest property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
	}
	auto schema_val = yyjson_obj_get(obj, "schema");
	if (!schema_val) {
		return "CreateTableRequest required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		string location_tmp;
		if (yyjson_is_str(location_val)) {
			location_tmp = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'location_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
		location = std::move(location_tmp);
	}
	auto partition_spec_val = yyjson_obj_get(obj, "partition-spec");
	if (partition_spec_val) {
		PartitionSpec partition_spec_tmp;
		error = partition_spec_tmp.TryFromJSON(partition_spec_val);
		if (!error.empty()) {
			return error;
		}
		partition_spec = std::move(partition_spec_tmp);
	}
	auto write_order_val = yyjson_obj_get(obj, "write-order");
	if (write_order_val) {
		SortOrder write_order_tmp;
		error = write_order_tmp.TryFromJSON(write_order_val);
		if (!error.empty()) {
			return error;
		}
		write_order = std::move(write_order_tmp);
	}
	auto stage_create_val = yyjson_obj_get(obj, "stage-create");
	if (stage_create_val) {
		bool stage_create_tmp;
		if (yyjson_is_bool(stage_create_val)) {
			stage_create_tmp = yyjson_get_bool(stage_create_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'stage_create_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(stage_create_val));
		}
		stage_create = std::move(stage_create_tmp);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CreateTableRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CreateTableRequest property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void CreateTableRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: name
	yyjson_mut_obj_add_str(doc, obj, "name", name.c_str());

	// Serialize: schema
	yyjson_mut_val *schema_val = schema.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "schema", schema_val);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		yyjson_mut_obj_add_str(doc, obj, "location", location_value.c_str());
	}

	// Serialize: partition-spec
	if (partition_spec.has_value()) {
		auto &partition_spec_value = *partition_spec;
		yyjson_mut_val *partition_spec_value_val = partition_spec_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "partition-spec", partition_spec_value_val);
	}

	// Serialize: write-order
	if (write_order.has_value()) {
		auto &write_order_value = *write_order;
		yyjson_mut_val *write_order_value_val = write_order_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "write-order", write_order_value_val);
	}

	// Serialize: stage-create
	if (stage_create.has_value()) {
		auto &stage_create_value = *stage_create;
		yyjson_mut_obj_add_bool(doc, obj, "stage-create", stage_create_value);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			yyjson_mut_obj_add_str(doc, properties_value_obj, key.c_str(), value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}
}

yyjson_mut_val *CreateTableRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
