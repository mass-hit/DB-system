//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

#include <type/value_factory.h>

namespace bustub {

struct PartitionKey {
 std::vector<Value> keys_;
 auto operator==(const PartitionKey &other) const -> bool {
  for (uint32_t i = 0; i < other.keys_.size(); i++) {
   if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
    return false;
   }
  }
  return true;
 }
 auto operator<(const PartitionKey &other) const -> bool {
  for (uint32_t i = 0; i < other.keys_.size(); i++) {
   if (keys_[i].CompareLessThan(other.keys_[i]) == CmpBool::CmpTrue) {
    return true;
   }
  }
  return false;
 }
};
}

namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::PartitionKey> {
 auto operator()(const bustub::PartitionKey &partition_key) const -> std::size_t {
  size_t curr_hash = 0;
  for (const auto &key : partition_key.keys_) {
   if (!key.IsNull()) {
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
   }
  }
  return curr_hash;
 }
};

}  // namespace std

namespace bustub {

class SimpleWindowHashTable {
public:
 SimpleWindowHashTable(const WindowFunctionType window_function_type,bool order_by_flag):window_function_type_(window_function_type),order_by_flag_(order_by_flag){}

 void CombineWindowValues(std::vector<Value> *result, const Value &input) {
  switch (window_function_type_) {
   case WindowFunctionType::CountAggregate:
    case WindowFunctionType::CountStarAggregate:
     if(result->empty()) {
      result->emplace_back(ValueFactory::GetIntegerValue(1));
     }else {
      if(order_by_flag_) {
       result->emplace_back(result->back().Add(ValueFactory::GetIntegerValue(1)));
      }else {
       for(auto& i:*result) {
        i=i.Add(ValueFactory::GetIntegerValue(1));
       }
       result->emplace_back(result->back().Copy());
      }
     }
   break;
   case WindowFunctionType::MaxAggregate:
    if(result->empty()) {
     result->emplace_back(input);
    }else {
     if(order_by_flag_) {
      result->emplace_back(result->back().Max(input));
     }else {
      for(auto& i:*result) {
       i=i.Max(input);
      }
      result->emplace_back(result->back().Copy());
     }
    }
   break;
   case WindowFunctionType::MinAggregate:
    if(result->empty()) {
     result->emplace_back(input);
    }else {
     if(order_by_flag_) {
      result->emplace_back(result->back().Min(input));
     }else {
      for(auto& i:*result) {
       i=i.Min(input);
      }
      result->emplace_back(result->back().Copy());
     }
    }
   break;
   case WindowFunctionType::SumAggregate:
    if(result->empty()) {
     result->emplace_back(input);
    }else {
     if(order_by_flag_) {
      result->emplace_back(result->back().Add(input));
     }else {
      for(auto& i:*result) {
       i=i.Add(input);
      }
      result->emplace_back(result->back().Copy());
     }
    }
   break;
   case WindowFunctionType::Rank:
    if(result->empty()) {
     result->emplace_back(ValueFactory::GetIntegerValue(1));
    }else {
     if(input.CompareEquals(last_.back())==CmpBool::CmpFalse) {
      result->emplace_back(ValueFactory::GetIntegerValue(static_cast<int>(last_.size())+1));
     }else {
      result->emplace_back(result->back().Copy());
     }
    }
   last_.emplace_back(input);
  }
 }

 void InsertCombine(const PartitionKey &partition_key, const Value &val) {
  if (ht_.count(partition_key) == 0) {
   ht_.insert({partition_key, {}});
  }
  CombineWindowValues(&ht_[partition_key], val);
 }

 auto Get(const PartitionKey &key)->std::vector<Value> *{
  if(ht_.count(key) == 0) {
   return nullptr;
  }
  return &ht_.at(key);
 }

 void Clear(){ht_.clear();}

 auto GetLast(size_t idx) -> Value { return last_[idx]; }


 class Iterator {
 public:
  explicit Iterator(std::map<PartitionKey, std::vector<Value>>::const_iterator iter) : iter_{iter} {}

  /** @return The key of the iterator */
  auto Key() -> const PartitionKey & { return iter_->first; }

  /** @return The value of the iterator */
  auto Val() -> const std::vector<Value>& { return iter_->second; }

  /** @return The iterator before it is incremented */
  auto operator++() -> Iterator & {
   ++iter_;
   return *this;
  }

  /** @return `true` if both iterators are identical */
  auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

  /** @return `true` if both iterators are different */
  auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

 private:
  /** Aggregates map */
  std::map<PartitionKey, std::vector<Value>>::const_iterator iter_;
 };

 /** @return Iterator to the start of the hash table */
 auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

 /** @return Iterator to the end of the hash table */
 auto End() -> Iterator { return Iterator{ht_.cend()}; }

private:
 std::map<PartitionKey, std::vector<Value>> ht_{};
 std::vector<Value> last_{};
 WindowFunctionType window_function_type_;
 bool order_by_flag_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:

 auto MakePartitionKey(const Tuple *tuple,uint32_t idx) -> PartitionKey {
  std::vector<Value> keys;
  for (const auto &expr : plan_->window_functions_.at(idx).partition_by_) {
   keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
  }
  return {keys};
 }

 auto MakePartitionValue(const Tuple *tuple,uint32_t idx) -> Value {
  return plan_->window_functions_.at(idx).type_==WindowFunctionType::Rank?plan_->window_functions_.at(idx).order_by_[0].second->Evaluate(tuple,child_executor_->GetOutputSchema()):plan_->window_functions_.at(idx).function_->Evaluate(tuple,child_executor_->GetOutputSchema());
 }


  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;


 std::vector<SimpleWindowHashTable> wht_list_;

 std::vector<Tuple> tuples_;

 std::unordered_map<uint32_t, size_t> index_map_;

 std::vector<SimpleWindowHashTable::Iterator> wht_iter_;

 std::vector<std::vector<Value>::const_iterator> val_iter_;

 size_t cur_;
};
}  // namespace bustub
