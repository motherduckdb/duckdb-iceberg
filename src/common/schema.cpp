#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

// https://iceberg.apache.org/spec/#schemas

// forward declaration
static LogicalType ParseTypeValue(rest_api_objects::Type &type);

static LogicalType ParseStruct(rest_api_objects::StructType &type) {
	child_list_t<LogicalType> children;
	for (auto &field : type.fields) {
		// NOTE: 'id', 'required', 'doc', 'initial_default', 'write_default' are ignored for now
		auto name = field->name;
		auto type = ParseTypeValue(*field->type);
		children.push_back(std::make_pair(name, type));
	}
	return LogicalType::STRUCT(std::move(children));
}

static LogicalType ParseList(rest_api_objects::ListType &type) {
	// NOTE: 'element-id', 'element-required' are ignored for now
	auto child_type = ParseTypeValue(*type.element);
	return LogicalType::LIST(child_type);
}

static LogicalType ParseMap(rest_api_objects::MapType &type) {
	// NOTE: 'key-id', 'value-id', 'value-required' are ignored for now
	auto key_type = ParseTypeValue(*type.key);
	auto value_type = ParseTypeValue(*type.value);
	return LogicalType::MAP(key_type, value_type);
}

static LogicalType ParseTypeValue(rest_api_objects::Type &type) {
	if (type.has_struct_type) {
		return ParseStruct(type.struct_type);
	} else if (type.has_list_type) {
		return ParseList(type.list_type);
	} else if (type.has_map_type) {
		return ParseMap(type.map_type);
	}

	if (!type.has_primitive_type) {
		throw InternalException("Invalid type encountered!");
	}
	auto &type_str = type.primitive_type.value;

	if (type_str == "boolean") {
		return LogicalType::BOOLEAN;
	}
	if (type_str == "int") {
		return LogicalType::INTEGER;
	}
	if (type_str == "long") {
		return LogicalType::BIGINT;
	}
	if (type_str == "float") {
		return LogicalType::FLOAT;
	}
	if (type_str == "double") {
		return LogicalType::DOUBLE;
	}
	if (type_str == "date") {
		return LogicalType::DATE;
	}
	if (type_str == "time") {
		return LogicalType::TIME;
	}
	if (type_str == "timestamp") {
		return LogicalType::TIMESTAMP;
	}
	if (type_str == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (type_str == "string") {
		return LogicalType::VARCHAR;
	}
	if (type_str == "uuid") {
		return LogicalType::UUID;
	}
	if (StringUtil::StartsWith(type_str, "fixed")) {
		// FIXME: use fixed size type in DuckDB
		return LogicalType::BLOB;
	}
	if (type_str == "binary") {
		return LogicalType::BLOB;
	}
	if (StringUtil::StartsWith(type_str, "decimal")) {
		D_ASSERT(type_str[7] == '(');
		D_ASSERT(type_str.back() == ')');
		auto start = type_str.find('(');
		auto end = type_str.rfind(')');
		auto raw_digits = type_str.substr(start + 1, end - start);
		auto digits = StringUtil::Split(raw_digits, ',');
		D_ASSERT(digits.size() == 2);

		auto width = std::stoi(digits[0]);
		auto scale = std::stoi(digits[1]);
		return LogicalType::DECIMAL(width, scale);
	}
	throw InvalidConfigurationException("Encountered an unrecognized type in JSON schema: \"%s\"", type_str);
}

IcebergColumnDefinition IcebergColumnDefinition::ParseFromJson(rest_api_objects::StructField &field) {
	IcebergColumnDefinition ret;

	ret.id = field.id;
	ret.name = field.name;
	ret.type = ParseTypeValue(*field.type);
	//! FIXME: use 'initial_default' instead
	ret.default_value = Value(ret.type);
	ret.required = field.required;

	return ret;
}

static vector<IcebergColumnDefinition> ParseSchemaFromJson(yyjson_val *schema_json) {
	auto parsed_schema = rest_api_objects::Schema::FromJSON(schema_json);
	auto &struct_type = parsed_schema.struct_type;

	vector<IcebergColumnDefinition> ret;
	for (auto &field : struct_type.fields) {
		ret.push_back(IcebergColumnDefinition::ParseFromJson(*field));
	}
	return ret;
}

vector<IcebergColumnDefinition> IcebergSnapshot::ParseSchema(vector<yyjson_val *> &schemas, idx_t schema_id) {
	// Multiple schemas can be present in the json metadata 'schemas' list
	for (const auto &schema_ptr : schemas) {
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema_ptr, "schema-id");
		if (found_schema_id == schema_id) {
			return ParseSchemaFromJson(schema_ptr);
		}
	}

	throw InvalidConfigurationException("Iceberg schema with schema id " + to_string(schema_id) + " was not found!");
}

} // namespace duckdb
