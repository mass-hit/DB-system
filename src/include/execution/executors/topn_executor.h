//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

#include <stack>

namespace bustub {
struct TupleComparator {
 TupleComparator(const TopNPlanNode *plan, const Schema *schema)
     : plan_(plan), schema_(schema) {}

 auto operator()(const Tuple &a, const Tuple &b) const->bool {
  const auto &order_by = plan_->GetOrderBy();
  for(const auto &order:order_by) {
   auto type=order.first;
   auto expression=order.second;
   Value value1=expression->Evaluate(&a,plan_->OutputSchema());
   Value value2=expression->Evaluate(&b,plan_->OutputSchema());
   if(value1.CompareEquals(value2)==CmpBool::CmpTrue) {
    continue;
   }
   if(type==OrderByType::ASC||type==OrderByType::DEFAULT) {
    return value1.CompareLessThan(value2)==CmpBool::CmpTrue;
   }
   return value1.CompareGreaterThan(value2)==CmpBool::CmpTrue;
  }
  return false;
 }

 const TopNPlanNode *plan_;
 const Schema *schema_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
public:
 /**
  * Construct a new TopNExecutor instance.
  * @param exec_ctx The executor context
  * @param plan The TopN plan to be executed
  */
 TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

 /** Initialize the TopN */
 void Init() override;

 /**
  * Yield the next tuple from the TopN.
  * @param[out] tuple The next tuple produced by the TopN
  * @param[out] rid The next tuple RID produced by the TopN
  * @return `true` if a tuple was produced, `false` if there are no more tuples
  */
 auto Next(Tuple *tuple, RID *rid) -> bool override;

 /** @return The output schema for the TopN */
 auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 /** Sets new child executor (for testing only) */
 void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
  child_executor_ = std::move(child_executor);
 }

 /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
 auto GetNumInHeap() -> size_t;

private:
 /** The TopN plan node to be executed */
 const TopNPlanNode *plan_;
 /** The child executor from which tuples are obtained */
 std::unique_ptr<AbstractExecutor> child_executor_;
 std::stack<Tuple> tuples_;
};
}  // namespace bustub
