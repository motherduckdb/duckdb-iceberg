#include "common/iceberg_default.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

IcebergDefaultBinder::IcebergDefaultBinder(ClientContext &context)
    : context(context), binder(Binder::CreateBinder(context)), constant_binder(*binder, context, "DEFAULT") {
}

Value IcebergDefaultBinder::Evaluate(optional_ptr<const ParsedExpression> expr, const LogicalType &type) {
	if (!expr) {
		return Value(type);
	}
	auto expr_copy = expr->Copy();
	auto bound_expr = constant_binder.Bind(expr_copy, nullptr);

	if (!bound_expr->IsFoldable()) {
		throw NotImplementedException("Only foldable expressions are allowed as DEFAULT values");
	}
	auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr, false).DefaultCastAs(type);

	auto type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::VARIANT:
	// case LogicalTypeId::GEOGRAPHY:
	case LogicalTypeId::GEOMETRY: {
		if (!val.IsNull()) {
			//! SPEC: All columns of unknown, variant, geometry, and geography types must default to null. Non-null
			//! values for initial-default or write-default are invalid.
			throw InvalidInputException("Non-null DEFAULT values are not accepted for columns of type %s",
			                            IcebergTypeHelper::LogicalTypeToIcebergType(type));
		}
		break;
	}
	default:
		break;
	};
	return val;
}

} // namespace duckdb
