#include "execution/executors/window_function_executor.h"

#include <tuple>

#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  cur_=0;
  wht_list_.clear();
  wht_iter_.clear();
  val_iter_.clear();
  index_map_.clear();
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(tuples_.begin(),tuples_.end(),[&](const Tuple &a,const Tuple &b) {
    for(const auto &order:plan_->window_functions_.begin()->second.order_by_) {
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
  for(const auto& [idx,window_function]:plan_->window_functions_) {
    index_map_.insert({idx,wht_list_.size()});
    wht_list_.emplace_back(window_function.type_,!window_function.order_by_.empty());
    for(const auto& cur_tuple:tuples_) {
      wht_list_.back().InsertCombine(MakePartitionKey(&cur_tuple,idx),MakePartitionValue(&cur_tuple,idx));
    }
    wht_iter_.push_back(wht_list_.back().Begin());
    val_iter_.push_back(wht_iter_.back().Val().begin());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cur_<tuples_.size()) {
    std::vector<Value> values;
    values.reserve(plan_->columns_.size());
    for(size_t i=0;i<plan_->columns_.size();i++) {
      values.emplace_back();
    }
    for(const auto& [idx,window_function]:plan_->window_functions_) {
      auto id=index_map_[idx];
      auto& wht_iter=wht_iter_[id];
      auto& val_iter=val_iter_[id];
      if(val_iter==wht_iter.Val().end()) {
        ++wht_iter;
        val_iter=wht_iter.Val().begin();
      }
      if(window_function.type_==WindowFunctionType::Rank) {
        values[idx-1]=wht_list_[id].GetLast(std::distance(wht_iter.Val().begin(),val_iter));
      }
      values[idx]=*val_iter;
      ++val_iter;
    }
    *tuple={values,&GetOutputSchema()};
    ++cur_;
    return true;
  }
  return false;
}
}  // namespace bustub
