//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

#include <common/util/hash_util.h>

namespace bustub {
struct HashJoinKey {
 std::vector<Value> keys;
 auto operator==(const HashJoinKey &other) const -> bool {
  for (uint32_t i = 0; i < other.keys.size(); i++) {
   if (keys[i].CompareEquals(other.keys[i]) != CmpBool::CmpTrue) {
    return false;
   }
  }
  return true;
 }
};
}

namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
 auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
  size_t curr_hash = 0;
  for (const auto &key : join_key.keys) {
   if (!key.IsNull()) {
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
   }
  }
  return curr_hash;
 }
};

}  // namespace std

namespace bustub {

class SimpleHashJoinTable {
public:
 void InsertKey(const HashJoinKey &key, const Tuple &tuple) {
  if (ht_.count(key) == 0) {
   ht_.insert({key, {tuple}});
  }else {
   ht_.at(key).push_back(tuple);
  }
 }

 auto Get(const HashJoinKey &key)->std::vector<Tuple> *{
  if(ht_.count(key) == 0) {
   return nullptr;
  }
  return &ht_.at(key);
 }

  void Clear(){ht_.clear();}

private:
 std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
 SimpleHashJoinTable hjt_{};
 std::unique_ptr<AbstractExecutor> left_child_;
 std::unique_ptr<AbstractExecutor> right_child_;
 Tuple left_tuple_;
 RID left_rid_;
 bool left_valid_;
 bool right_flag_;
 std::vector<Tuple>* right_tuples_;
 std::vector<Tuple>::iterator right_iter_;
};

}  // namespace bustub
