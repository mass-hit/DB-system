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
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int rows_updated = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    table_info_->table_->UpdateTupleMeta({0,true},child_rid);
    for(auto index_info:index_infos) {
      index_info->index_->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),child_rid,exec_ctx_->GetTransaction());
    }
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple inserted_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    auto result=table_info_->table_->InsertTuple({0,false},inserted_tuple, exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction(),plan_->GetTableOid());
    if (result!=std::nullopt) {
      ++rows_updated;
      for(auto index_info:index_infos) {
        index_info->index_->InsertEntry(inserted_tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),result.value(),exec_ctx_->GetTransaction());
      }
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
