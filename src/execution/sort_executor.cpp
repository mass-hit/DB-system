#include "execution/executors/sort_executor.h"

#include <tuple>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  auto order_by=plan_->GetOrderBy();
  std::sort(tuples_.begin(),tuples_.end(),[&](const Tuple &a,const Tuple &b)->bool {
    for(const auto &order:order_by) {
      auto type=order.first;
      auto expression=order.second;
      Value value1=expression->Evaluate(&a,plan_->OutputSchema());
      Value value2=expression->Evaluate(&b,plan_->OutputSchema());
      if(value1.CompareEquals(value2)==CmpBool::CmpTrue) {
        continue;
      }
      if (type==OrderByType::ASC||type==OrderByType::DEFAULT) {
        return value1.CompareLessThan(value2)==CmpBool::CmpTrue;
      }
      return value1.CompareGreaterThan(value2)==CmpBool::CmpTrue;
    }
    return false;
  });
  cur_=tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(cur_ != tuples_.end()) {
    *tuple = *cur_;
    *rid=tuple->GetRid();
    ++cur_;
    return true;
  }
  return false;
}

}  // namespace bustub
