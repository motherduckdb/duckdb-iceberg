#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/iceberg_type.hpp"
#include "utils/json_utils.hpp"
#include "catalog_utils.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

IcebergCreateTableRequest::IcebergCreateTableRequest(shared_ptr<IcebergTableSchema> schema, string table_name)
    : table_name(table_name), initial_schema(schema) {
}

rest_api_objects::CreateTableRequest CreateUpdateCreateTableRequest() {
	rest_api_objects::CreateTableRequest create_table_request;

	create_table_request.name = "table_name";
	//	create_table_request.schema = ;
	create_table_request.location = "some file location";
	create_table_request.has_location = true;
	create_table_request.has_partition_spec = false;
	create_table_request.has_write_order = false;
	create_table_request.stage_create = true;
	create_table_request.has_stage_create = true;
	create_table_request.has_properties = false;
	return create_table_request;
}

// rest_api_objects::TableUpdate IcebergAddSnapshot::CreateSetSnapshotRefUpdate() {
//	rest_api_objects::TableUpdate table_update;
//
//	table_update.has_set_snapshot_ref_update = true;
//	auto &update = table_update.set_snapshot_ref_update;
//	update.base_update.action = "set-snapshot-ref";
//	update.has_action = true;
//	update.action = "set-snapshot-ref";
//
//	update.ref_name = "main";
//	update.snapshot_reference.type = "branch";
//	update.snapshot_reference.snapshot_id = snapshot.snapshot_id;
//	return table_update;
//}

void IcebergCreateTableRequest::CreateCreateTableRequest(DatabaseInstance &db, ClientContext &context,
                                                         IcebergCommitState &commit_state) {
	//	auto &system_catalog = Catalog::GetSystemCatalog(db);
	//	auto data = CatalogTransaction::GetSystemTransaction(db);
	//	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	//	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	//	D_ASSERT(avro_copy_p);
	//	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;
	//
	//	auto manifest_length = manifest_file::WriteToFile(table_info, manifest_file, avro_copy, db, context);
	//	manifest.manifest_length = manifest_length;
	//
	//	D_ASSERT(manifest_list.manifests.empty());
	//	manifest_list.manifests = std::move(commit_state.manifests);
	//	manifest_list.manifests.push_back(std::move(manifest));
	//	manifest_list::WriteToFile(manifest_list, avro_copy, db, context);
	//	commit_state.manifests = std::move(manifest_list.manifests);
	//
	//	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate());
}

static void PopulateYYJSONfields(yyjson_mut_doc *doc, yyjson_mut_val *fields_array, IcebergColumnDefinition &column) {
	auto field_obj = yyjson_mut_arr_add_obj(doc, fields_array);
	yyjson_mut_obj_add_uint(doc, field_obj, "id", column.id);
	yyjson_mut_obj_add_strcpy(doc, field_obj, "name", column.name.c_str());
	if (column.type.IsNested()) {
		auto nested_type = yyjson_mut_obj_add_obj(doc, field_obj, "type");
		switch (column.type.id()) {
		case LogicalTypeId::STRUCT: {
			yyjson_mut_obj_add_strcpy(doc, nested_type, "type", "struct");
			auto nested_fields_arr = yyjson_mut_obj_add_arr(doc, nested_type, "fields");
			for (auto &field : column.children) {
				PopulateYYJSONfields(doc, nested_fields_arr, *field);
			}
			break;
		}
		case LogicalTypeId::LIST: {
			yyjson_mut_obj_add_strcpy(doc, nested_type, "type", "list");
			D_ASSERT(column.children.size() == 1);
			yyjson_mut_obj_add_uint(doc, nested_type, "element-id", column.children[0]->id);
			yyjson_mut_obj_add_strcpy(doc, nested_type, "element",
			                          IcebergTypeRenamer::GetIcebergTypeString(column.children[0]->type).c_str());
			yyjson_mut_obj_add_bool(doc, nested_type, "element-required", false);
			break;
		}
		default:
			throw NotImplementedException("Not implemented");
		}
	} else {
		yyjson_mut_obj_add_strcpy(doc, field_obj, "type",
		                          IcebergTypeRenamer::GetIcebergTypeString(column.type).c_str());
	}
	yyjson_mut_obj_add_bool(doc, field_obj, "required", column.required);
	// skip doc, initial_default, and write_default for now.
	//	yyjson_mut_obj_add_strcpy(doc, field_obj, "doc", "string");
	//	yyjson_mut_obj_add_bool(doc, field_obj, "initial_default", true);
	//	yyjson_mut_obj_add_bool(doc, field_obj, "write_default", true);
}

shared_ptr<IcebergTableSchema> IcebergCreateTableRequest::CreateIcebergSchema(const ICTableEntry *table_entry) {
	auto schema = make_shared_ptr<IcebergTableSchema>();
	// should this be a different schema id?
	schema->schema_id = table_entry->table_info->table_metadata.current_schema_id;

	// TODO: this can all be refactored out
	//  this makes the IcebergTableSchema, and we use that to dump data to JSON.
	//  we can just directly dump it to json.
	auto column_iterator = table_entry->GetColumns().Logical();
	// TODO: should this start at 1?
	idx_t column_id = 1;
	for (auto column = column_iterator.begin(); column != column_iterator.end(); ++column) {
		auto name = (*column).Name();
		// TODO: is this correct?
		auto field_id = column_id;
		column_id++;
		bool required = false;
		auto logical_type = (*column).GetType();
		auto type = IcebergTypeHelper::CreateIcebergRestType(logical_type, column_id);
		auto column_def = IcebergColumnDefinition::ParseType(name, field_id, required, type, nullptr);

		schema->columns.push_back(std::move(column_def));
	}
	return schema;
}

string IcebergCreateTableRequest::CreateTableToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object) {

	auto schema = initial_schema;

	//! name
	yyjson_mut_obj_add_strcpy(doc, root_object, "name", table_name.c_str());
	//! location (apparently not needed)
	// yyjson_mut_obj_add_strcpy(doc, root_object, "location", "s3://warehouse/default/this_is_a_new_table");

	auto schema_json = yyjson_mut_obj_add_obj(doc, root_object, "schema");
	yyjson_mut_obj_add_strcpy(doc, schema_json, "type", "struct");

	auto fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "fields");

	// populate the fields
	for (auto &field : schema->columns) {
		PopulateYYJSONfields(doc, fields_arr, *field);
	}

	yyjson_mut_obj_add_uint(doc, schema_json, "schema-id", schema->schema_id);
	auto identifier_fields_arr = yyjson_mut_obj_add_arr(doc, schema_json, "identifier-field-ids");

	auto partition_spec = yyjson_mut_obj_add_obj(doc, root_object, "partition-spec");
	yyjson_mut_obj_add_uint(doc, partition_spec, "spec-id", 0);
	auto partition_spec_fields = yyjson_mut_obj_add_arr(doc, partition_spec, "fields");

	auto write_order = yyjson_mut_obj_add_obj(doc, root_object, "write-order");
	yyjson_mut_obj_add_uint(doc, write_order, "order-id", 0);
	auto write_order_fields = yyjson_mut_obj_add_arr(doc, write_order, "fields");

	yyjson_mut_obj_add_bool(doc, root_object, "stage-create", false);
	auto properties = yyjson_mut_obj_add_obj(doc, root_object, "properties");

	return JSONUtils::JsonDocToString(doc);
}

} // namespace duckdb
