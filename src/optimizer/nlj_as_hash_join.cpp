#include <execution/expressions/logic_expression.h>

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void Extract(const AbstractExpressionRef &predicate,std::vector<AbstractExpressionRef>* left_key_expressions,std::vector<AbstractExpressionRef>* right_key_expressions) {
  auto logic_expression= dynamic_cast<LogicExpression*>(predicate.get());
  if(logic_expression!=nullptr) {
    Extract(logic_expression->GetChildAt(0),left_key_expressions,right_key_expressions);
    Extract(logic_expression->GetChildAt(1),left_key_expressions,right_key_expressions);
  }
  auto comparison_expression= dynamic_cast<ComparisonExpression*>(predicate.get());
  if(comparison_expression!=nullptr) {
    auto column_value_0= dynamic_cast<const ColumnValueExpression&>(*comparison_expression->GetChildAt(0));
    auto column_value_1=dynamic_cast<const ColumnValueExpression&>(*comparison_expression->GetChildAt(1));
    if(column_value_0.GetTupleIdx()==0) {
      left_key_expressions->emplace_back(comparison_expression->GetChildAt(0));
      right_key_expressions->emplace_back(comparison_expression->GetChildAt(1));
    }else {
      left_key_expressions->emplace_back(comparison_expression->GetChildAt(1));
      right_key_expressions->emplace_back(comparison_expression->GetChildAt(0));
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    auto predicate=nlj_plan.Predicate();
    Extract(predicate, &left_key_expressions, &right_key_expressions);
    return std::make_unique<HashJoinPlanNode>(nlj_plan.output_schema_,nlj_plan.GetLeftPlan(),nlj_plan.GetRightPlan(),left_key_expressions,right_key_expressions,nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
