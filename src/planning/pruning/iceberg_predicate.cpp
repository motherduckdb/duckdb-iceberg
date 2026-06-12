#include "planning/pruning/iceberg_predicate.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"

namespace duckdb {

template <class TRANSFORM>
bool MatchBoundsTemplated(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform);

template <class TRANSFORM>
static bool MatchBoundsConstant(const Value &constant, ExpressionType comparison_type,
                                const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	auto constant_value = TRANSFORM::ApplyTransform(constant, transform);

	if (stats.BoundsAreNull()) {
		// bounds are actually null, expression is not a null comparison expression
		// those are handled in MatchBoundsTemplated
		// So we can return false since no remaining expression type will match a null value
		D_ASSERT(comparison_type != ExpressionType::OPERATOR_IS_NOT_NULL);
		D_ASSERT(comparison_type != ExpressionType::OPERATOR_IS_NULL);
		D_ASSERT(comparison_type != ExpressionType::COMPARE_DISTINCT_FROM);
		D_ASSERT(comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		return false;
	}

	if (!stats.has_upper_bounds || !stats.has_lower_bounds) {
		// we do not have upper or lower bounds, assume the file matches.
		return true;
	}

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return TRANSFORM::CompareEqual(constant_value, stats);
	case ExpressionType::COMPARE_GREATERTHAN:
		return TRANSFORM::CompareGreaterThan(constant_value, stats);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return TRANSFORM::CompareGreaterThanOrEqual(constant_value, stats);
	case ExpressionType::COMPARE_LESSTHAN:
		return TRANSFORM::CompareLessThan(constant_value, stats);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return TRANSFORM::CompareLessThanOrEqual(constant_value, stats);
	default:
		//! Conservative approach: we don't know, so we just say it's not filtered out
		return true;
	}
}

template <class TRANSFORM>
static bool MatchBoundsConstantFilter(const ConstantFilter &constant_filter, const IcebergPredicateStats &stats,
                                      const IcebergTransform &transform) {
	return MatchBoundsConstant<TRANSFORM>(constant_filter.constant, constant_filter.comparison_type, stats, transform);
}

template <class TRANSFORM>
static bool MatchBoundsIsNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsIsNotNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_not_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsConjunctionAndFilter(ClientContext &context, const ConjunctionAndFilter &conjunction_and,
                                            const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	for (auto &child : conjunction_and.child_filters) {
		if (!MatchBoundsTemplated<TRANSFORM>(context, *child, stats, transform)) {
			return false;
		}
	}
	return true;
}

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

template <class TRANSFORM>
bool MatchBoundsTemplated(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform) {
	//! TODO: support more filter types
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return MatchBoundsConstantFilter<TRANSFORM>(constant_filter, stats, transform);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		return MatchBoundsConjunctionAndFilter<TRANSFORM>(context, conjunction_and_filter, stats, transform);
	}
	case TableFilterType::IS_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::IS_NOT_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			return MatchBoundsTemplated<TRANSFORM>(context, *optional_filter.child_filter, stats, transform);
		}
		//! child filter wasn't populated (yet?) for some reason, just be conservative
		return true;
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		D_ASSERT(!in_filter.values.empty());
		for (auto &value : in_filter.values) {
			if (MatchBoundsConstant<TRANSFORM>(value, ExpressionType::COMPARE_EQUAL, stats, transform)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::EXPRESSION_FILTER: {
		//! Expressions can be arbitrarily complex, and we currently only support IS NULL/IS NOT NULL checks against the
		//! column itself, i.e. where the expression is a BOUND_OPERATOR with type OPERATOR_IS_NULL/_IS_NOT_NULL with a
		//! single child expression of type BOUND_REF.
		//!
		//! See duckdb/duckdb-iceberg#464
		auto &expression_filter = filter.Cast<ExpressionFilter>();

		//! Spatial predicates on a geometry column (e.g. ST_Intersects, which the spatial
		//! optimizer rewrites into a `geom && <const>` / st_intersects_extent bbox pre-filter)
		//! arrive here as an ExpressionFilter. Delegate to GeometryStats::CheckZonemap, which
		//! whitelists the bbox-prunable predicates and does the intersect/contain math against
		//! the file's bounding-box extent. Only prune when the result is provably empty.
		if (stats.geometry_stats) {
			auto result = GeometryStats::CheckZonemap(*stats.geometry_stats, expression_filter.expr);
			return result != FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		auto &expr = *expression_filter.expr;

		switch (expr.type) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_EQUAL: {
			auto &compare_expr = expr.Cast<BoundComparisonExpression>();
			auto comparison_type = compare_expr.GetExpressionType();
			auto &left = *compare_expr.left;
			auto &right = *compare_expr.right;
			if (IsDirectReference(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				return MatchBoundsConstant<TRANSFORM>(right.Cast<BoundConstantExpression>().value, comparison_type,
				                                      stats, transform);
			}
			if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
				return MatchBoundsConstant<TRANSFORM>(left.Cast<BoundConstantExpression>().value,
				                                      FlipComparisonExpression(comparison_type), stats, transform);
			}
			return true;
		}
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			D_ASSERT(expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR);
			auto &bound_operator_expr = expr.Cast<BoundOperatorExpression>();

			D_ASSERT(bound_operator_expr.children.size() == 1);
			auto &child_expr = bound_operator_expr.children[0];
			if (child_expr->type != ExpressionType::BOUND_REF) {
				//! We can't evaluate expressions that aren't direct column references
				return true;
			}

			if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
				return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
			}
			D_ASSERT(expr.type == ExpressionType::OPERATOR_IS_NOT_NULL);
			return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
		}
		// TODO: Implement ExpressionType::BOUND_BETWEEN and COMPARE_IN.
		// https://github.com/duckdblabs/duckdb-internal/issues/8497
		default:
			return true;
		}
	}
	default:
		//! Conservative approach: we don't know what this is, just say it doesn't filter anything
		return true;
	}
}

bool IcebergPredicate::MatchBounds(ClientContext &context, const TableFilter &filter,
                                   const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return MatchBoundsTemplated<IdentityTransform>(context, filter, stats, transform);
	case IcebergTransformType::BUCKET:
		return MatchBoundsTemplated<BucketTransform>(context, filter, stats, transform);
	case IcebergTransformType::TRUNCATE:
		return MatchBoundsTemplated<TruncateTransform>(context, filter, stats, transform);
	case IcebergTransformType::YEAR:
		return MatchBoundsTemplated<YearTransform>(context, filter, stats, transform);
	case IcebergTransformType::MONTH:
		return MatchBoundsTemplated<MonthTransform>(context, filter, stats, transform);
	case IcebergTransformType::DAY:
		return MatchBoundsTemplated<DayTransform>(context, filter, stats, transform);
	case IcebergTransformType::HOUR:
		return MatchBoundsTemplated<HourTransform>(context, filter, stats, transform);
	case IcebergTransformType::VOID:
		return true;
	default:
		throw InvalidConfigurationException("Transform '%s' not implemented", transform.RawType());
	}
}

} // namespace duckdb
