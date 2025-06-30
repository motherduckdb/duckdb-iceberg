#include "duckdb/common/string_util.hpp"
#include "storage/iceberg_type.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"
#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

string IcebergTypeRenamer::GetIcebergTypeString(LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL:
		return "decimal";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::STRUCT:
		return "struct";
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return "list";
	case LogicalTypeId::MAP:
	default:
		throw NotImplementedException("Type not supported in Duckdb-Iceberg");
	}
}

// if (type.has_primitive_type) {
//	res->type = ParsePrimitiveType(type.primitive_type);
//} else if (type.has_struct_type) {
//	auto &struct_type = type.struct_type;
//	child_list_t<LogicalType> struct_children;
//	for (auto &field_p : struct_type.fields) {
//		auto &field = *field_p;
//		auto child = ParseType(field.name, field.id, field.required, *field.type,
//							   field.has_initial_default ? &field.initial_default : nullptr);
//		struct_children.push_back(std::make_pair(child->name, child->type));
//		res->children.push_back(std::move(child));
//	}
//	res->type = LogicalType::STRUCT(std::move(struct_children));
//} else if (type.has_list_type) {
//	auto &list_type = type.list_type;
//	auto child =
//		ParseType("element", list_type.element_id, list_type.element_required, *list_type.element, nullptr);
//	res->type = LogicalType::LIST(child->type);
//	res->children.push_back(std::move(child));
//} else if (type.has_map_type) {
//	auto &map_type = type.map_type;
//	auto key = ParseType("key", map_type.key_id, true, *map_type.key, nullptr);
//	auto value = ParseType("value", map_type.value_id, map_type.value_required, *map_type.value, nullptr);
//	res->type = LogicalType::MAP(key->type, value->type);
//	res->children.push_back(std::move(key));
//	res->children.push_back(std::move(value));
//} else {
//	throw InvalidConfigurationException("Encountered an invalid type in JSON schema");
//}

rest_api_objects::Type IcebergTypeHelper::CreateIcebergRestType(LogicalType &type, idx_t &column_id) {
	rest_api_objects::Type rest_type;

	switch (type.id()) {
	case LogicalTypeId::MAP: {
		throw InvalidInputException("whatever");
		//		type.has_map_type = true;
		//		type.map_type = rest_api_objects::MapType();
		//		auto aux = type.AuxInfo()->Cast<Map>();
		//		for (auto &child : aux->child_types) {
		//
		//		}
	}
	case LogicalTypeId::STRUCT: {
		rest_type.has_primitive_type = false;
		rest_type.has_struct_type = true;
		rest_type.struct_type = rest_api_objects::StructType();
		auto stuct_aux = type.AuxInfo()->Cast<StructTypeInfo>();
		for (auto &child : stuct_aux.child_types) {
			auto struct_child = make_uniq<rest_api_objects::StructField>();
			struct_child->name = child.first;
			struct_child->type =
			    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(child.second, column_id));
			struct_child->has_doc = false;
			struct_child->required = false;
			struct_child->has_initial_default = false;
			struct_child->id = column_id;
			column_id++;
			rest_type.struct_type.fields.push_back(std::move(struct_child));
		}
		return rest_type;
	}
	case LogicalTypeId::LIST: {
		rest_type.has_primitive_type = false;
		rest_type.has_list_type = true;
		rest_type.list_type = rest_api_objects::ListType();
		auto list_aux = type.AuxInfo()->Cast<ListTypeInfo>();
		rest_type.list_type.type = IcebergTypeRenamer::GetIcebergTypeString(list_aux.child_type);
		rest_type.list_type.element =
		    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(list_aux.child_type, column_id));
		rest_type.list_type.element_required = false;
		// TODO: what is the element id for? Is it the field id?
		rest_type.list_type.element_id = column_id;
		column_id++;
		return rest_type;
	}
	default:
		break;
	}
	rest_type.has_primitive_type = true;
	rest_type.primitive_type = rest_api_objects::PrimitiveType();
	rest_type.primitive_type.value = IcebergTypeRenamer::GetIcebergTypeString(type);
	return rest_type;
}

} // namespace duckdb
