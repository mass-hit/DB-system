//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

#include <concurrency/transaction_manager.h>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  flag_=false;
  txn_=exec_ctx_->GetTransaction();
  txn_manager_=exec_ctx_->GetTransactionManager();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int rows_inserted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<RID> rids;
    for(auto index:index_infos) {
      index->index_->ScanKey(child_tuple.KeyFromTuple(table_info->schema_,index->key_schema_,index->index_->GetKeyAttrs()),&rids,txn_);
      if(!rids.empty()) {
        break;
      }
    }
    if(rids.empty()) {
      auto result=table_info->table_->InsertTuple({txn_->GetTransactionTempTs(),false},child_tuple, exec_ctx_->GetLockManager(),txn_,plan_->GetTableOid());
      txn_manager_->UpdateVersionLink(*result, VersionUndoLink{UndoLink{}, true});
      if (result!=std::nullopt) {
        ++rows_inserted;
        for(auto index_info:index_infos) {
          if(!index_info->index_->InsertEntry(child_tuple.KeyFromTuple(table_info->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),result.value(),txn_)) {
            txn_->SetTainted();
            throw ExecutionException("write conflict");
          }
        }
      }
      txn_->AppendWriteSet(plan_->table_oid_,result.value());
    }else {
      *rid=rids.front();
      auto meta=table_info->table_->GetTupleMeta(*rid);
      if(meta.is_deleted_) {
        if(meta.ts_==txn_->GetTransactionTempTs()) {
          meta.is_deleted_=false;
          table_info->table_->UpdateTupleInPlace(meta,child_tuple,*rid);
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
          table_info->table_->UpdateTupleInPlace(meta,child_tuple,*rid);
        }
        txn_->AppendWriteSet(plan_->table_oid_,*rid);
      }else {
        txn_->SetTainted();
        throw ExecutionException("write conflict");
      }
    }
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, rows_inserted);
  *tuple=Tuple{values,&GetOutputSchema()};
  if(!flag_) {
    flag_=true;
    return true;
  }
  return false;
}

}  // namespace bustub
