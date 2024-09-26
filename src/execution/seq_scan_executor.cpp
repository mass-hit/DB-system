//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include <concurrency/transaction_manager.h>
#include <execution/execution_common.h>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
  txn_=exec_ctx_->GetTransaction();
  txn_manager_=exec_ctx_->GetTransactionManager();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    auto [tuple_meta, current_tuple] = table_iter_->GetTuple();
    *rid = table_iter_->GetRID();
    if(tuple_meta.ts_==txn_->GetTransactionTempTs()||((tuple_meta.ts_&TXN_START_ID)==0&&tuple_meta.ts_<=txn_->GetReadTs())) {
      if (!tuple_meta.is_deleted_&&(plan_->filter_predicate_==nullptr||plan_->filter_predicate_->Evaluate(&current_tuple, table_info_->schema_).GetAs<bool>())) {
        *tuple = current_tuple;
        ++(*table_iter_);
        return true;
      }
    }else {
      auto undo_link=txn_manager_->GetUndoLink(*rid);
      std::vector<UndoLog>undo_logs;
      while(undo_link!=std::nullopt&&undo_link->IsValid()) {
        auto undo_log=txn_manager_->GetUndoLog(*undo_link);
        undo_logs.push_back(undo_log);
        if(undo_log.ts_<=txn_->GetReadTs()) {
          auto re_tuple=ReconstructTuple(&GetOutputSchema(),current_tuple,tuple_meta,undo_logs);
          if (re_tuple!=std::nullopt&&(plan_->filter_predicate_==nullptr||plan_->filter_predicate_->Evaluate(&(*re_tuple), table_info_->schema_).GetAs<bool>())) {
            *tuple = *re_tuple;
            ++(*table_iter_);
            return true;
          }
          break;
        }
        undo_link=undo_log.prev_version_;
      }
    }
    ++(*table_iter_);
  }
  return false;
}

}  // namespace bustub
