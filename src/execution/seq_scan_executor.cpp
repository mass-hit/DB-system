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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    auto [tuple_meta, current_tuple] = table_iter_->GetTuple();
    if (!tuple_meta.is_deleted_&&(plan_->filter_predicate_==nullptr||plan_->filter_predicate_->Evaluate(&current_tuple, table_info_->schema_).GetAs<bool>())) {
      *tuple = current_tuple;
      *rid = table_iter_->GetRID();
      ++(*table_iter_);
      return true;
    }
    ++(*table_iter_);
  }
  return false;
}

}  // namespace bustub
