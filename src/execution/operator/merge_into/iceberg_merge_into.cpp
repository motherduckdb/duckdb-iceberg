
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

#include "execution/operator/merge_into/iceberg_merge_insert.hpp"
#include "execution/operator/merge_into/iceberg_merge_update.hpp"
#include "execution/operator/merge_into/iceberg_merge_into.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "execution/operator/iceberg_update.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

namespace duckdb {

namespace {

vector<LogicalType> AddMergeRowIdType(const vector<LogicalType> &input_types, idx_t row_id_start) {
	auto result = input_types;
	result.insert(result.begin() + row_id_start, LogicalType::BIGINT);
	return result;
}

class IcebergMergeRowIdGlobalState : public GlobalOperatorState {
public:
	mutex lock;
	value_map_t<row_t> row_ids;
	row_t next_row_id = 0;
};

//! PhysicalMergeInto expects one BIGINT row-id for duplicate-match detection, while Iceberg row identity is a
//! tuple of virtual columns. Insert a query-local BIGINT key immediately before the Iceberg row-id columns. The
//! original columns remain at the end of the chunk, preserving the layout expected by IcebergUpdate/Delete.
class IcebergMergeRowId : public PhysicalOperator {
public:
	IcebergMergeRowId(PhysicalPlan &physical_plan, const vector<LogicalType> &input_types, idx_t row_id_start)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, AddMergeRowIdType(input_types, row_id_start),
	                       1),
	      row_id_start(row_id_start) {
	}

	string GetName() const override {
		return "ICEBERG_MERGE_ROW_ID";
	}

	bool ParallelOperator() const override {
		return true;
	}

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<IcebergMergeRowIdGlobalState>();
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
	                           GlobalOperatorState &gstate_p, OperatorState &state) const override {
		for (idx_t col_idx = 0; col_idx < row_id_start; col_idx++) {
			output.data[col_idx].Reference(input.data[col_idx]);
		}
		for (idx_t col_idx = row_id_start; col_idx < input.ColumnCount(); col_idx++) {
			output.data[col_idx + 1].Reference(input.data[col_idx]);
		}

		auto &merge_row_ids = output.data[row_id_start];
		merge_row_ids.SetVectorType(VectorType::FLAT_VECTOR);
		auto merge_row_id_data = FlatVector::GetDataMutable<row_t>(merge_row_ids);
		auto &validity = FlatVector::ValidityMutable(merge_row_ids);
		validity.SetAllValid(input.size());

		auto &gstate = gstate_p.Cast<IcebergMergeRowIdGlobalState>();
		lock_guard<mutex> guard(gstate.lock);
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			vector<Value> row_id;
			row_id.reserve(input.ColumnCount() - row_id_start);
			for (idx_t col_idx = row_id_start; col_idx < input.ColumnCount(); col_idx++) {
				row_id.push_back(input.GetValue(col_idx, row_idx));
			}
			if (row_id[0].IsNull()) {
				validity.SetInvalid(row_idx);
				continue;
			}

			auto iceberg_row_id = Value::TUPLE(std::move(row_id));
			auto entry = gstate.row_ids.find(iceberg_row_id);
			if (entry == gstate.row_ids.end()) {
				auto merge_row_id = gstate.next_row_id++;
				entry = gstate.row_ids.emplace(std::move(iceberg_row_id), merge_row_id).first;
			}
			merge_row_id_data[row_idx] = entry->second;
		}
		output.SetChildCardinality(input.size());
		return OperatorResultType::NEED_MORE_INPUT;
	}

private:
	idx_t row_id_start;
};

} // namespace

void IcebergMergeInto::ProjectAndCastForCopy(ClientContext &context, DataChunk &input_chunk, PhysicalOperator &copy_op,
                                             ExpressionExecutor *expression_executor, DataChunk &projected_chunk,
                                             DataChunk &cast_chunk) {
	reference<DataChunk> chunk_ref = input_chunk;
	if (expression_executor) {
		projected_chunk.Reset();
		expression_executor->Execute(input_chunk, projected_chunk);
		chunk_ref = projected_chunk;
	}
	auto &copy_types = copy_op.Cast<PhysicalCopyToFile>().expected_types;
	for (idx_t i = 0; i < chunk_ref.get().ColumnCount(); i++) {
		if (chunk_ref.get().data[i].GetType() != copy_types[i]) {
			VectorOperations::Cast(context, chunk_ref.get().data[i], cast_chunk.data[i], chunk_ref.get().size());
		} else {
			cast_chunk.data[i].Reference(chunk_ref.get().data[i]);
		}
	}
	cast_chunk.SetChildCardinality(chunk_ref.get().size());
}

namespace {

class CaptureEvent : public Event {
public:
	explicit CaptureEvent(Executor &executor) : Event(executor) {
	}

	void Schedule() override {
		// No-op
	}

	shared_ptr<Event> GetCapturedEvent() {
		if (parents.empty()) {
			return nullptr;
		}
		return parents[0].lock();
	}
};

} // namespace

SinkFinalizeType IcebergMergeInto::FinalizeCopyToInsert(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        PhysicalOperator &copy_op, PhysicalOperator &insert_op,
                                                        InterruptState &interrupt_state) {
	OperatorSinkFinalizeInput copy_finalize {*copy_op.sink_state, interrupt_state};
	CaptureEvent capture_event(pipeline.executor);
	auto finalize_result = copy_op.Finalize(pipeline, capture_event, context, copy_finalize);
	if (finalize_result == SinkFinalizeType::BLOCKED) {
		return finalize_result;
	}

	//! PhysicalCopyToFile creates an Event as part of Finalize
	//! We need to intercept and execute this event's tasks, to finish the finalize work
	auto captured_event = capture_event.GetCapturedEvent();
	if (captured_event) {
		// Schedule the event's tasks with our token
		captured_event->Schedule();

		// Work on tasks until the event is finished
		shared_ptr<Task> task;
		auto &token = pipeline.executor.GetToken();
		while (!captured_event->IsFinished()) {
			if (TaskScheduler::GetScheduler(context).GetTaskFromProducer(token, task)) {
				task->Execute(TaskExecutionMode::PROCESS_ALL);
				task.reset();
			}
		}
	}

	DataChunk chunk;
	chunk.Initialize(context, copy_op.types);

	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);

	auto copy_global = copy_op.GetGlobalSourceState(context);
	auto copy_local = copy_op.GetLocalSourceState(exec_context, *copy_global);
	OperatorSourceInput source_input {*copy_global, *copy_local, interrupt_state};

	auto insert_global = insert_op.GetGlobalSinkState(context);
	auto insert_local = insert_op.GetLocalSinkState(exec_context);
	OperatorSinkInput sink_input {*insert_global, *insert_local, interrupt_state};
	SourceResultType source_res = SourceResultType::HAVE_MORE_OUTPUT;
	while (source_res == SourceResultType::HAVE_MORE_OUTPUT) {
		chunk.Reset();
		source_res = copy_op.GetData(exec_context, chunk, source_input);
		if (chunk.size() == 0) {
			continue;
		}
		if (source_res == SourceResultType::BLOCKED) {
			throw InternalException("BLOCKED not supported in IcebergMerge");
		}

		auto sink_result = insert_op.Sink(exec_context, chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("BLOCKED not supported in IcebergMerge");
		}
	}
	OperatorSinkCombineInput combine_input {*insert_global, *insert_local, interrupt_state};
	auto combine_res = insert_op.Combine(exec_context, combine_input);
	if (combine_res == SinkCombineResultType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMerge");
	}
	OperatorSinkFinalizeInput finalize_input {*insert_global, interrupt_state};
	auto finalize_res = insert_op.Finalize(pipeline, event, context, finalize_input);
	if (finalize_res == SinkFinalizeType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMerge");
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
static unique_ptr<MergeIntoOperator> IcebergPlanMergeIntoAction(IcebergCatalog &catalog, ClientContext &context,
                                                                LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                                BoundMergeIntoAction &action,
                                                                PhysicalOperator &child_plan) {
	auto result = make_uniq<MergeIntoOperator>();

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}
	auto return_types = op.types;

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, catalog);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	auto iceberg_version = table_metadata.iceberg_version;

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		LogicalUpdate update(op.table);
		for (auto &def : op.bound_defaults) {
			update.bound_defaults.push_back(def->Copy());
		}
		update.bound_constraints = std::move(bound_constraints);
		update.expressions = std::move(action.expressions);
		update.columns = std::move(action.columns);
		update.update_is_del_and_insert = action.update_is_del_and_insert;

		IcebergCopyInput copy_input(context, table_metadata, schema);
		if (iceberg_version >= 3) {
			copy_input.virtual_columns = IcebergInsertVirtualColumns::WRITE_ROW_ID;
		}

		auto &update_op =
		    IcebergUpdate::PlanUpdateOperator(context, planner, updated_table_entry, update, child_plan, copy_input);

		// The row_id comes before the deletion information, that is always the 3 last column of the chunk.
		if (table_metadata.iceberg_version >= 3) {
			update_op.row_id_index = child_plan.types.size() - 4;
		}

		// plan copy and insert
		auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input);
		auto &copy_op = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert_op = IcebergInsert::PlanInsert(context, planner, updated_table_entry).Cast<IcebergInsert>();
		insert_op.children.push_back(copy_op);
		insert_op.update_delete_op = update_op.delete_op;

		// wrap in IcebergMergeUpdate
		auto &merge_update =
		    planner.Make<IcebergMergeUpdate>(return_types, update_op, copy_op, insert_op).Cast<IcebergMergeUpdate>();
		merge_update.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_update;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		LogicalDelete delete_op(op.table, TableIndex(0));

		// we only push 2 columns for positional deletes
		idx_t column_offset = 0;
		if (iceberg_version >= 3) {
			delete_op.expressions.push_back(nullptr);
			//! The row ids of the table contain the _row_id column, which we're not interested in
			column_offset = 1;
		}
		vector<LogicalType> row_id_types {LogicalType::VARCHAR, LogicalType::BIGINT};
		for (idx_t i = 0; i < 2; i++) {
			auto ref = make_uniq<BoundReferenceExpression>(row_id_types[i], op.row_id_start + i + column_offset + 1);
			delete_op.expressions.push_back(std::move(ref));
		}
		delete_op.bound_constraints = std::move(bound_constraints);
		result->op = catalog.PlanDelete(context, planner, delete_op, child_plan);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		LogicalInsert insert_op(op.table, TableIndex(0));
		insert_op.bound_constraints = std::move(bound_constraints);
		for (auto &def : op.bound_defaults) {
			insert_op.bound_defaults.push_back(def->Copy());
		}
		//! DEPRECATED: transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);

		IcebergCopyInput copy_input(context, table_metadata, schema);
		auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input);
		auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert = IcebergInsert::PlanInsert(context, planner, table_entry);
		insert.children.push_back(physical_copy);

		auto &merge_insert =
		    planner.Make<IcebergMergeInsert>(insert.types, insert, physical_copy).Cast<IcebergMergeInsert>();
		merge_insert.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_insert;
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not implemented for Iceberg yet");
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	// plan the merge into clauses
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
				if (update_delete_count > 1) {
					throw NotImplementedException(
					    "MERGE INTO with Iceberg only supports a single UPDATE/DELETE action currently");
				}
			}
			planned_actions.push_back(IcebergPlanMergeIntoAction(*this, context, op, planner, *action, plan));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &merge_row_id = planner.Make<IcebergMergeRowId>(plan.types, op.row_id_start);
	merge_row_id.children.push_back(plan);

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               true, op.return_chunk);
	result.children.push_back(merge_row_id);
	return result;
}

} // namespace duckdb
