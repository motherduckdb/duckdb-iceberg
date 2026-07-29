
#include "rest_catalog/objects/expression.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Expression::Expression() {
}

Expression Expression::FromJSON(JSONValue obj) {
	Expression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Expression Expression::Copy() const {
	Expression res;
	if (boolean_expression.has_value()) {
		res.boolean_expression.emplace();
		(*res.boolean_expression) = (*boolean_expression).Copy();
	}
	if (true_expression.has_value()) {
		res.true_expression.emplace();
		(*res.true_expression) = (*true_expression).Copy();
	}
	if (false_expression.has_value()) {
		res.false_expression.emplace();
		(*res.false_expression) = (*false_expression).Copy();
	}
	if (and_or_expression.has_value()) {
		res.and_or_expression.emplace();
		(*res.and_or_expression) = (*and_or_expression).Copy();
	}
	if (not_expression.has_value()) {
		res.not_expression.emplace();
		(*res.not_expression) = (*not_expression).Copy();
	}
	if (set_expression.has_value()) {
		res.set_expression.emplace();
		(*res.set_expression) = (*set_expression).Copy();
	}
	if (literal_expression.has_value()) {
		res.literal_expression.emplace();
		(*res.literal_expression) = (*literal_expression).Copy();
	}
	if (unary_expression.has_value()) {
		res.unary_expression.emplace();
		(*res.unary_expression) = (*unary_expression).Copy();
	}
	return res;
}

string Expression::TryFromJSON(JSONValue obj) {
	string error;
	do {
		boolean_expression.emplace();
		error = boolean_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			boolean_expression = nullopt;
		}
		true_expression.emplace();
		error = true_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			true_expression = nullopt;
		}
		false_expression.emplace();
		error = false_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			false_expression = nullopt;
		}
		and_or_expression.emplace();
		error = and_or_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			and_or_expression = nullopt;
		}
		not_expression.emplace();
		error = not_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			not_expression = nullopt;
		}
		set_expression.emplace();
		error = set_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			set_expression = nullopt;
		}
		literal_expression.emplace();
		error = literal_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			literal_expression = nullopt;
		}
		unary_expression.emplace();
		error = unary_expression->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			unary_expression = nullopt;
		}
		return "Expression failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Expression::ToJSON(JSONWriter &writer) const {
	if (boolean_expression.has_value()) {
		return boolean_expression->ToJSON(writer);
	} else if (true_expression.has_value()) {
		return true_expression->ToJSON(writer);
	} else if (false_expression.has_value()) {
		return false_expression->ToJSON(writer);
	} else if (and_or_expression.has_value()) {
		return and_or_expression->ToJSON(writer);
	} else if (not_expression.has_value()) {
		return not_expression->ToJSON(writer);
	} else if (set_expression.has_value()) {
		return set_expression->ToJSON(writer);
	} else if (literal_expression.has_value()) {
		return literal_expression->ToJSON(writer);
	} else if (unary_expression.has_value()) {
		return unary_expression->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
