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

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  flag_=false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int rows_inserted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto result=table_info->table_->InsertTuple({0,false},child_tuple, exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction(),plan_->GetTableOid());
    if (result!=std::nullopt) {
      ++rows_inserted;
      for(auto index_info:index_infos) {
        index_info->index_->InsertEntry(child_tuple.KeyFromTuple(table_info->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),result.value(),exec_ctx_->GetTransaction());
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
