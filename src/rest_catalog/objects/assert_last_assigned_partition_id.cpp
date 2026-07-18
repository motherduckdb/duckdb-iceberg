
#include "rest_catalog/objects/assert_last_assigned_partition_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertLastAssignedPartitionId::AssertLastAssignedPartitionId() {
}

AssertLastAssignedPartitionId AssertLastAssignedPartitionId::FromJSON(yyjson_val *obj) {
	AssertLastAssignedPartitionId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertLastAssignedPartitionId AssertLastAssignedPartitionId::Copy() const {
	AssertLastAssignedPartitionId res;
	res.table_requirement = table_requirement.Copy();
	res.last_assigned_partition_id = last_assigned_partition_id;
	return res;
}

string AssertLastAssignedPartitionId::TryFromJSON(yyjson_val *obj) {
	string error;
	error = table_requirement.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto last_assigned_partition_id_val = yyjson_obj_get(obj, "last-assigned-partition-id");
	if (!last_assigned_partition_id_val) {
		return "AssertLastAssignedPartitionId required property 'last-assigned-partition-id' is missing";
	} else {
		if (yyjson_is_int(last_assigned_partition_id_val)) {
			last_assigned_partition_id = yyjson_get_int(last_assigned_partition_id_val);
		} else {
			return StringUtil::Format("AssertLastAssignedPartitionId property 'last_assigned_partition_id' is not of "
			                          "type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(last_assigned_partition_id_val));
		}
	}
	return "";
}

void AssertLastAssignedPartitionId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: TableRequirement
	table_requirement.PopulateJSON(doc, obj);

	// Serialize: last-assigned-partition-id
	yyjson_mut_obj_add_int(doc, obj, "last-assigned-partition-id", last_assigned_partition_id);
}

yyjson_mut_val *AssertLastAssignedPartitionId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
