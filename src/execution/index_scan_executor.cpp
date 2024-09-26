//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

#include <concurrency/transaction_manager.h>
#include <execution/execution_common.h>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {}

void IndexScanExecutor::Init() {
  rids_.clear();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_=exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  htable->ScanKey(Tuple{{dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get())->val_},index_info->index_->GetKeySchema()},&rids_,exec_ctx_->GetTransaction());
  rid_iter_=rids_.begin();
  txn_=exec_ctx_->GetTransaction();
  txn_manager_=exec_ctx_->GetTransactionManager();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (rid_iter_!=rids_.end()) {
    auto [tuple_meta, current_tuple] = table_info_->table_->GetTuple(*rid_iter_);
    *rid = *rid_iter_;
    if(tuple_meta.ts_==txn_->GetTransactionTempTs()||((tuple_meta.ts_&TXN_START_ID)==0&&tuple_meta.ts_<=txn_->GetReadTs())) {
      if (!tuple_meta.is_deleted_) {
        *tuple = current_tuple;
        ++rid_iter_;
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
          if (re_tuple!=std::nullopt) {
            *tuple = *re_tuple;
            ++rid_iter_;
            return true;
          }
          break;
        }
        undo_link=undo_log.prev_version_;
      }
    }
    ++rid_iter_;
  }
  return false;
}

}  // namespace bustub
