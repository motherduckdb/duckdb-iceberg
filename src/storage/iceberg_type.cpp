#include "duckdb/common/string_util.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"
#include "rest_catalog/objects/struct_field.hpp"
#include "rest_catalog/objects/type.hpp"

namespace duckdb {

string IcebergTypeHelper::GetIcebergTypeString(LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL: {
		auto aux = type.AuxInfo()->Cast<DecimalTypeInfo>();
		return StringUtil::Format("decimal(%d, %d)", aux.width, aux.scale);
	}
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::STRUCT:
		return "struct";
	case LogicalTypeId::ARRAY: {
		// TODO: get extra type info. How long is the arry
		return "list";
		// auto aux = type.AuxInfo()->Cast<ArrayTypeInfo>();
		// auto length = aux.size;
		// auto child = aux.child_type;
		// return StringUtil::Format("fixed(%d)", length);
	}
	case LogicalTypeId::LIST:
		return "list";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";

	case LogicalTypeId::MAP:
		return "map";
	default:
		throw NotImplementedException("Type %s not supported in Iceberg", LogicalTypeIdToString(type.id()));
	}
}

rest_api_objects::Type IcebergTypeHelper::CreateIcebergRestType(LogicalType &type, idx_t &column_id) {
	rest_api_objects::Type rest_type;

	switch (type.id()) {
	case LogicalTypeId::MAP: {
		rest_type.has_primitive_type = false;
		rest_type.has_map_type = true;
		rest_type.map_type = rest_api_objects::MapType();
		auto aux = type.AuxInfo()->Cast<ListTypeInfo>();
		auto child_type = aux.child_type.AuxInfo()->Cast<StructTypeInfo>();
		// How do I get the child types for a map type?
		// maybe both should just be string?
		auto key_type = child_type.child_types.front().second;
		rest_type.map_type.key_id = static_cast<int32_t>(column_id);
		column_id++;
		rest_type.map_type.key = make_uniq<rest_api_objects::Type>(
		    IcebergTypeHelper::CreateIcebergRestType(child_type.child_types.front().second, column_id));
		rest_type.map_type.value_id = static_cast<int32_t>(column_id);
		column_id++;
		rest_type.map_type.value = make_uniq<rest_api_objects::Type>(
		    IcebergTypeHelper::CreateIcebergRestType(child_type.child_types.back().second, column_id));
		rest_type.map_type.value_required = false;

		return rest_type;
	}
	case LogicalTypeId::STRUCT: {
		rest_type.has_primitive_type = false;
		rest_type.has_struct_type = true;
		rest_type.struct_type = rest_api_objects::StructType();
		auto stuct_aux = type.AuxInfo()->Cast<StructTypeInfo>();
		for (auto &child : stuct_aux.child_types) {
			auto struct_child = make_uniq<rest_api_objects::StructField>();
			struct_child->name = child.first;
			struct_child->id = column_id;
			column_id++;
			struct_child->type =
			    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(child.second, column_id));
			struct_child->has_doc = false;
			struct_child->required = false;
			struct_child->has_initial_default = false;
			rest_type.struct_type.fields.push_back(std::move(struct_child));
		}
		return rest_type;
	}
	case LogicalTypeId::LIST: {
		rest_type.has_primitive_type = false;
		rest_type.has_list_type = true;
		rest_type.list_type = rest_api_objects::ListType();
		auto list_aux = type.AuxInfo()->Cast<ListTypeInfo>();
		rest_type.list_type.type = IcebergTypeHelper::GetIcebergTypeString(list_aux.child_type);
		rest_type.list_type.element_id = column_id;
		column_id++;
		rest_type.list_type.element =
		    make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(list_aux.child_type, column_id));
		rest_type.list_type.element_required = false;
		// TODO: what is the element id for? Is it the field id?
		return rest_type;
	}
	case LogicalTypeId::ARRAY: {
		throw InvalidConfigurationException("Array type not supported in Iceberg type. Please cast to LIST");
		// rest_type.has_primitive_type = false;
		// rest_type.has_list_type = true;
		// rest_type.list_type = rest_api_objects::ListType();
		// auto array_aux = type.AuxInfo()->Cast<ArrayTypeInfo>();
		// rest_type.list_type.type = IcebergTypeHelper::GetIcebergTypeString(array_aux.child_type);
		// rest_type.list_type.element_id = column_id;
		// column_id++;
		// rest_type.list_type.element =
		// 	make_uniq<rest_api_objects::Type>(IcebergTypeHelper::CreateIcebergRestType(array_aux.child_type,
		// column_id)); rest_type.list_type.element_required = false;
		// // TODO: what is the element id for? Is it the field id?
		// return rest_type;
	}
	default:
		break;
	}
	rest_type.has_primitive_type = true;
	rest_type.primitive_type = rest_api_objects::PrimitiveType();
	rest_type.primitive_type.value = IcebergTypeHelper::GetIcebergTypeString(type);
	return rest_type;
}

} // namespace duckdb
