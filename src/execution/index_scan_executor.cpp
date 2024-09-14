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

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {}

void IndexScanExecutor::Init() {
  rids_.clear();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_=exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  htable_->ScanKey(Tuple{{dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get())->val_},index_info->index_->GetKeySchema()},&rids_,exec_ctx_->GetTransaction());
  rid_iter_=rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (rid_iter_!=rids_.end()) {
    auto [tuple_meta, current_tuple] = table_info_->table_->GetTuple(*rid_iter_);
    if (!tuple_meta.is_deleted_) {
      *tuple = current_tuple;
      *rid = *rid_iter_;
      ++rid_iter_;
      return true;
    }
    ++rid_iter_;
  }
  return false;
}

}  // namespace bustub
