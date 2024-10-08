#include <execution/plans/limit_plan.h>
#include <execution/plans/sort_plan.h>
#include <execution/plans/topn_plan.h>

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    if(limit_plan.children_[0]->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*limit_plan.children_[0]);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_,optimized_plan->children_[0],sort_plan.GetOrderBy(),limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
