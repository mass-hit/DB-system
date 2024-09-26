//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts=last_commit_ts_+1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for(auto& [table_oid,rids]:txn->write_set_) {
    auto& table=catalog_->GetTable(table_oid)->table_;
    for(auto rid:rids) {
      auto tuple_meta=table->GetTupleMeta(rid);
      tuple_meta.ts_=commit_ts;
      table->UpdateTupleMeta(tuple_meta,rid);
    }
  }


  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_.store(commit_ts);
  ++last_commit_ts_;


  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::set<txn_id_t> txn_ids;
  for(const auto& i:txn_map_) {
    txn_ids.insert(i.first);
  }
  for(const auto& name:catalog_->GetTableNames()) {
    auto& table=catalog_->GetTable(name)->table_;
    auto it=table->MakeIterator();
    while(!it.IsEnd()) {
      auto[tuple_meta,tuple]=it.GetTuple();
      auto rid=it.GetRID();
      timestamp_t water_mark=GetWatermark();
      if(tuple_meta.ts_>water_mark) {
        auto undo_link=GetUndoLink(rid);
        while(undo_link!=std::nullopt&&undo_link->IsValid()) {
          auto undo_log=GetUndoLog(*undo_link);
          auto txn_id=undo_link->prev_txn_;
          if(txn_ids.find(txn_id)!=txn_ids.end()) {
            txn_ids.erase(txn_id);
          }
          if(undo_log.ts_<=water_mark) {
            break;
          }
          undo_link=undo_log.prev_version_;
        }
      }
      ++it;
    }
  }
  for(auto txn_id:txn_ids) {
    auto txn=txn_map_.at(txn_id);
    if(txn->state_==TransactionState::COMMITTED||txn->state_==TransactionState::ABORTED) {
      txn_map_.erase(txn_id);
    }
  }
}
}  // namespace bustub
