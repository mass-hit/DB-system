//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

#include <concurrency/transaction_manager.h>
#include <execution/execution_common.h>

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  flag_=false;
  pk_=false;
  txn_=exec_ctx_->GetTransaction();
  txn_manager_=exec_ctx_->GetTransactionManager();
  for(auto index:exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    if(index->is_primary_key_) {
      pk_=true;
      break;
    }
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int rows_updated = 0;
  Tuple child_tuple;
  RID child_rid;
  if(pk_) {
    auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    std::vector<Tuple> tuples;
    while(child_executor_->Next(&child_tuple,&child_rid)) {
      TupleMeta meta=table_info_->table_->GetTupleMeta(child_rid);
      if(CheckWriteConflict(meta.ts_,txn_->GetReadTs(),txn_->GetTransactionTempTs())) {
        txn_->SetTainted();
        throw ExecutionException("write conflict");
      }
      TupleMeta inserted_meta={txn_->GetTransactionTempTs(),true};
      std::vector<bool> modified_fields;
      auto undo_link=txn_manager_->GetUndoLink(child_rid);
      if((meta.ts_&TXN_START_ID)==0) {
        for(size_t i=0;i<GetOutputSchema().GetColumnCount();++i) {
          modified_fields.push_back(true);
        }
        UndoLog undo_log={false,modified_fields,child_tuple,meta.ts_};
        if(undo_link!=std::nullopt&&undo_link->IsValid()) {
          undo_log.prev_version_=*undo_link;
        }
        undo_link=txn_->AppendUndoLog(undo_log);
        txn_manager_->UpdateVersionLink(*rid,VersionUndoLink{*undo_link, true});
      }
      table_info_->table_->UpdateTupleMeta(inserted_meta,child_rid);
      txn_->AppendWriteSet(plan_->table_oid_,child_rid);
      tuples.emplace_back(child_tuple);
      ++rows_updated;
    }
    for(const auto& old_tuple:tuples) {
      std::vector<RID> rids;
      std::vector<Value> values;
      values.reserve(plan_->target_expressions_.size());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
      }
      Tuple inserted_tuple=Tuple{values,&child_executor_->GetOutputSchema()};
      for(auto index:index_infos) {
        index->index_->ScanKey(inserted_tuple.KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs()),&rids,txn_);
        if(!rids.empty()) {
          break;
        }
      }
      if(rids.empty()) {
        auto result=table_info_->table_->InsertTuple({txn_->GetTransactionTempTs(),false},inserted_tuple, exec_ctx_->GetLockManager(),txn_,plan_->GetTableOid());
        if (result!=std::nullopt) {
          for(auto index_info:index_infos) {
            if(!index_info->index_->InsertEntry(inserted_tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),result.value(),txn_)) {
              txn_->SetTainted();
              throw ExecutionException("write conflict");
            }
          }
        }
        txn_->AppendWriteSet(plan_->table_oid_,result.value());
      }else {
        *rid=rids.front();
        auto meta=table_info_->table_->GetTupleMeta(*rid);
        if(meta.is_deleted_) {
          if(meta.ts_==txn_->GetTransactionTempTs()) {
            meta.is_deleted_=false;
            table_info_->table_->UpdateTupleInPlace(meta,inserted_tuple,*rid);
          }else {
            auto undo_link=txn_manager_->GetUndoLink(*rid);
            UndoLog undo_log={true,{},{},meta.ts_};
            if(undo_link!=std::nullopt&&undo_link->IsValid()) {
              undo_log.prev_version_=*undo_link;
            }
            undo_link=txn_->AppendUndoLog(undo_log);
            txn_manager_->UpdateVersionLink(*rid,VersionUndoLink{*undo_link, true});
            meta.is_deleted_=false;
            meta.ts_=txn_->GetTransactionTempTs();
            table_info_->table_->UpdateTupleInPlace(meta,inserted_tuple,*rid);
          }
          txn_->AppendWriteSet(plan_->table_oid_,*rid);
        }else {
          txn_->SetTainted();
          throw ExecutionException("write conflict");
        }
      }
    }
  }else {
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      TupleMeta meta=table_info_->table_->GetTupleMeta(child_rid);
      if(CheckWriteConflict(meta.ts_,txn_->GetReadTs(),txn_->GetTransactionTempTs())) {
        txn_->SetTainted();
        throw ExecutionException("write conflict");
      }
      TupleMeta inserted_meta={txn_->GetTransactionTempTs(),false};
      std::vector<bool> modified_fields;
      std::vector<Value> new_values;
      std::vector<Column> new_columns;
      std::vector<Value>values;
      auto undo_link=txn_manager_->GetUndoLink(child_rid);
      values.reserve(plan_->target_expressions_.size());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }
      Tuple inserted_tuple=Tuple{values,&child_executor_->GetOutputSchema()};
      if(IsTupleContentEqual(inserted_tuple,child_tuple)) {
        continue;
      }
      if((meta.ts_&TXN_START_ID)==0) {
        for(size_t i=0;i<plan_->target_expressions_.size();++i) {
          auto prev_value=child_tuple.GetValue(&child_executor_->GetOutputSchema(),i);
          auto new_value=inserted_tuple.GetValue(&child_executor_->GetOutputSchema(),i);
          if(!new_value.CompareExactlyEquals(prev_value)) {
            modified_fields.push_back(true);
            new_values.push_back(prev_value);
            new_columns.push_back(child_executor_->GetOutputSchema().GetColumn(i));
          }else {
            modified_fields.push_back(false);
          }
        }
        auto schema=Schema(new_columns);
        UndoLog undo_log={false,modified_fields,Tuple(new_values,&schema),meta.ts_};
        if(undo_link!=std::nullopt&&undo_link->IsValid()) {
          undo_log.prev_version_=*undo_link;
        }
        undo_link=txn_->AppendUndoLog(undo_log);
        txn_manager_->UpdateVersionLink(*rid,VersionUndoLink{*undo_link, true});
      }else if(undo_link!=std::nullopt&&undo_link->IsValid()) {
        auto prev_undo_log=txn_manager_->GetUndoLog(*undo_link);
        auto prev_tuple= ReconstructTuple(&child_executor_->GetOutputSchema(),child_tuple,meta,{prev_undo_log});
        if(prev_tuple!=std::nullopt) {
          for(size_t i=0;i<plan_->target_expressions_.size();++i) {
            auto prev_value=prev_tuple->GetValue(&child_executor_->GetOutputSchema(),i);
            auto new_value=inserted_tuple.GetValue(&child_executor_->GetOutputSchema(),i);
            if(!new_value.CompareExactlyEquals(prev_value)||prev_undo_log.modified_fields_[i]) {
              modified_fields.push_back(true);
              new_values.push_back(prev_value);
              new_columns.push_back(child_executor_->GetOutputSchema().GetColumn(i));
            }else {
              modified_fields.push_back(false);
            }
          }
          auto schema=Schema(new_columns);
          UndoLog undo_log={false,modified_fields,Tuple(new_values,&schema),prev_undo_log.ts_,prev_undo_log.prev_version_};
          txn_->ModifyUndoLog(0,undo_log);
        }
      }
      table_info_->table_->UpdateTupleInPlace(inserted_meta,inserted_tuple,child_rid);
      txn_->AppendWriteSet(plan_->table_oid_,child_rid);
      ++rows_updated;
    }
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, rows_updated);
  *tuple=Tuple{values,&GetOutputSchema()};
  if(!flag_) {
    flag_=true;
    return true;
  }
  return false;
}

}  // namespace bustub
