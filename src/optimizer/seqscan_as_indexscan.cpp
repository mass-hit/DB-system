#include <execution/expressions/column_value_expression.h>
#include <execution/expressions/comparison_expression.h>
#include <execution/plans/index_scan_plan.h>
#include <execution/plans/seq_scan_plan.h>

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if(seq_scan_plan.filter_predicate_ != nullptr) {
      auto cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
      if(cmp_expr != nullptr&&cmp_expr->comp_type_==ComparisonType::Equal) {
        auto index_infos=catalog_.GetTableIndexes(seq_scan_plan.table_name_);
        auto column_id = dynamic_cast<ColumnValueExpression *>(cmp_expr->children_[0].get())->GetColIdx();
        for(auto index_info:index_infos) {
          if(column_id==index_info->index_->GetKeyAttrs()[0]) {
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_scan_plan.table_oid_,
          index_info->index_oid_, seq_scan_plan.filter_predicate_);
          }
        }
      }
    }
  }
  return optimized_plan;
}
}  // namespace bustub
