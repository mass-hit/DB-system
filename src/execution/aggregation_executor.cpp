//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)),aht_(plan->GetAggregates(),plan->GetAggregateTypes()),aht_iterator_(aht_.Begin()) {
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple),MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();
  flag_=false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(aht_iterator_==aht_.End()) {
    if(!flag_&&aht_.Begin()==aht_.End()) {
      if(!plan_->GetGroupBys().empty()) {
        return false;
      }
      *tuple=Tuple{aht_.GenerateInitialAggregateValue().aggregates_,&GetOutputSchema()};
      *rid=tuple->GetRid();
      flag_=true;
      return true;
    }
    return false;
  }
  std::vector<Value>values;
  values.insert(values.end(),aht_iterator_.Key().group_bys_.begin(),aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(),aht_iterator_.Val().aggregates_.begin(),aht_iterator_.Val().aggregates_.end());
  *tuple=Tuple{values,&GetOutputSchema()};
  *rid=tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
