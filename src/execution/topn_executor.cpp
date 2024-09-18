#include "execution/executors/topn_executor.h"

#include <tuple>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = TupleComparator(plan_, &GetOutputSchema());
  std::priority_queue<Tuple,std::vector<Tuple>,TupleComparator> tuple_heap(cmp);
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    tuple_heap.push(tuple);
    if(tuple_heap.size()>plan_->GetN()) {
      tuple_heap.pop();
    }
  }
  while(!tuple_heap.empty()) {
    tuples_.emplace(tuple_heap.top());
    tuple_heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!tuples_.empty()) {
    *tuple = tuples_.top();
    *rid=tuple->GetRid();
    tuples_.pop();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
