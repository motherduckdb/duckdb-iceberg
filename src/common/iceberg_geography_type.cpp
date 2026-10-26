#include "common/iceberg_geography_type.hpp"

#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

static constexpr const char *GEOGRAPHY_TYPE_NAME = "ICEBERG_GEOGRAPHY";

static LogicalType GetFromParameters(const vector<string> &parameters);

LogicalType IcebergGeographyType::Get(const string &iceberg_type) {
	//! `geography`, or `geography(<crs>)`, or `geography(<crs>, <algorithm>)`
	vector<string> parameters;
	auto open = iceberg_type.find('(');
	if (open != string::npos) {
		if (iceberg_type.back() != ')') {
			throw InvalidConfigurationException("Invalid geography type format: %s", iceberg_type);
		}
		parameters = StringUtil::Split(iceberg_type.substr(open + 1, iceberg_type.size() - open - 2), ',');
		for (auto &parameter : parameters) {
			StringUtil::Trim(parameter);
		}
	} else if (iceberg_type != "geography") {
		throw InvalidConfigurationException("Invalid geography type format: %s", iceberg_type);
	}
	return GetFromParameters(parameters);
}

bool IcebergGeographyType::IsGeography(const LogicalType &type) {
	return type.id() == LogicalTypeId::BLOB && type.GetAlias() == GEOGRAPHY_TYPE_NAME;
}

bool IcebergGeographyType::ContainsGeography(const LogicalType &type) {
	return TypeVisitor::Contains(type, IsGeography);
}

static LogicalType GetFromParameters(const vector<string> &parameters) {
	auto info = make_uniq<ExtensionTypeInfo>();
	for (auto &parameter : parameters) {
		info->modifiers.emplace_back(Value(parameter));
	}
	return LogicalType(LogicalTypeId::BLOB).WithAlias(GEOGRAPHY_TYPE_NAME).WithExtensionInfo(std::move(info));
}

static LogicalType BindGeographyType(BindLogicalTypeInput &input) {
	vector<string> parameters;
	for (auto &modifier : input.modifiers) {
		parameters.push_back(modifier.GetValue().ToString());
	}
	return GetFromParameters(parameters);
}

//! Registered so that binding can resolve the type of an existing geography field (e.g. the NULL that INSERT
//! supplies for an omitted column). Declaring the type is refused by RejectDeclared.
void IcebergGeographyType::Register(ExtensionLoader &loader) {
	TypeConstructorSet constructors;
	for (idx_t parameter_count = 0; parameter_count <= 2; parameter_count++) {
		auto signature = TypeConstructor::Signature();
		for (idx_t i = 0; i < parameter_count; i++) {
			signature.AddParameter(i == 0 ? "crs" : "algorithm", LogicalType::VARCHAR);
		}
		constructors.AddFunction(TypeConstructor(std::move(signature), BindGeographyType));
	}
	loader.RegisterType(GEOGRAPHY_TYPE_NAME, LogicalType(LogicalTypeId::BLOB).WithAlias(GEOGRAPHY_TYPE_NAME),
	                    std::move(constructors));
}

string IcebergGeographyType::GetIcebergType(const LogicalType &type) {
	D_ASSERT(IsGeography(type));
	auto info = type.GetExtensionInfo();
	if (!info || info->modifiers.empty()) {
		return "geography";
	}
	vector<string> parameters;
	for (auto &modifier : info->modifiers) {
		parameters.push_back(modifier.value.ToString());
	}
	return "geography(" + StringUtil::Join(parameters, ", ") + ")";
}

static void ThrowDeclaredGeography(const Identifier &name) {
	throw NotImplementedException("Cannot declare %s: geography columns are not supported", name);
}

void IcebergGeographyType::RejectDeclared(ClientContext &context, LogicalOperator &op) {
	// EXPLAIN ANALYZE executes the DDL in its child plan.
	for (auto &child : op.children) {
		RejectDeclared(context, *child);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CREATE_TABLE: {
		auto &create = op.Cast<LogicalCreateTable>();
		for (auto &column : create.info->Base().columns.Logical()) {
			if (ContainsGeography(column.Type())) {
				ThrowDeclaredGeography(column.Name());
			}
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_CREATE_TYPE: {
		auto &info = op.Cast<LogicalCreate>().info->Cast<CreateTypeInfo>();
		if (ContainsGeography(info.type)) {
			ThrowDeclaredGeography(info.GetQualifiedName().Name());
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_ALTER: {
		auto &alter_info = *op.Cast<LogicalAlter>().info;
		if (alter_info.type != AlterType::ALTER_TABLE) {
			return;
		}
		auto &table_info = alter_info.Cast<AlterTableInfo>();
		switch (table_info.alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto &column = table_info.Cast<AddColumnInfo>().new_column;
			if (ContainsGeography(column.Type())) {
				ThrowDeclaredGeography(column.Name());
			}
			break;
		}
		case AlterTableType::ADD_FIELD: {
			auto &field = table_info.Cast<AddFieldInfo>().new_field;
			if (ContainsGeography(field.Type())) {
				ThrowDeclaredGeography(field.Name());
			}
			break;
		}
		case AlterTableType::ALTER_COLUMN_TYPE: {
			auto &change = table_info.Cast<ChangeColumnTypeInfo>();
			if (ContainsGeography(change.target_type) && !change.column_path.empty()) {
				ThrowDeclaredGeography(change.column_path.back());
			}
			break;
		}
		default:
			break;
		}
		return;
	}
	default:
		return;
	}
}

} // namespace duckdb
