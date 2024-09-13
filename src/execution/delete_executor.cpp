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

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
  child_executor_->Init();
  flag_=false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_infos=exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int rows_deleted = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    table_info->table_->UpdateTupleMeta({0,true},child_rid);
    for(auto index_info:index_infos) {
      index_info->index_->DeleteEntry(child_tuple.KeyFromTuple(table_info->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),child_rid,exec_ctx_->GetTransaction());
    }
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
