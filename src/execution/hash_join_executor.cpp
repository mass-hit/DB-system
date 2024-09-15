//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

#include <type/value_factory.h>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan),left_child_(std::move(left_child)),right_child_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hjt_.Clear();
  Tuple right_tuple;
  RID right_rid;
  left_valid_=left_child_->Next(&left_tuple_,&left_rid_);
  while(right_child_->Next(&right_tuple,&right_rid)) {
    std::vector<Value> values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(&right_tuple, right_child_->GetOutputSchema()));
    }
    hjt_.InsertKey({values},right_tuple);
  }
  right_flag_=false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(left_valid_) {
    std::vector<Value> values;
    if(right_flag_) {
      if(right_iter_!=right_tuples_->end()) {
        for(uint32_t i=0;i<left_child_->GetOutputSchema().GetColumnCount();++i) {
          values.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(),i));
        }
        for(uint32_t i=0;i<right_child_->GetOutputSchema().GetColumnCount();++i) {
          values.push_back(right_iter_->GetValue(&right_child_->GetOutputSchema(),i));
        }
        *tuple=Tuple{values,&GetOutputSchema()};
        ++right_iter_;
        return true;
      }
      left_valid_=left_child_->Next(&left_tuple_,&left_rid_);
      right_flag_=false;
    }else {
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        values.emplace_back(expr->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
      }
      auto result =hjt_.Get({values});
      if(result==nullptr) {
        if(plan_->GetJoinType() == JoinType::LEFT) {
          values.clear();
          for(uint32_t i=0;i<left_child_->GetOutputSchema().GetColumnCount();++i) {
            values.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(),i));
          }
          for(uint32_t i=0;i<right_child_->GetOutputSchema().GetColumnCount();++i) {
            values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
          }
          *tuple=Tuple{values,&GetOutputSchema()};
          left_valid_=left_child_->Next(&left_tuple_,&left_rid_);
          return true;
        }
        left_valid_=left_child_->Next(&left_tuple_,&left_rid_);
      }else {
        right_tuples_=result;
        right_iter_=right_tuples_->begin();
        right_flag_=true;
      }
    }
  }
  return false;
}

}  // namespace bustub
