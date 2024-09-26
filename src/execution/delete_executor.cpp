//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

#include <concurrency/transaction_manager.h>
#include <execution/execution_common.h>

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  flag_=false;
  txn_=exec_ctx_->GetTransaction();
  txn_manager_=exec_ctx_->GetTransactionManager();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int rows_deleted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
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
    ++rows_deleted;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, rows_deleted);
  *tuple=Tuple{values,&GetOutputSchema()};
  if(!flag_) {
    flag_=true;
    return true;
  }
  return false;
}

}  // namespace bustub
