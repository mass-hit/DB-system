//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <unistd.h>

#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t frame_id, size_t k): fid_(frame_id), k_(k) {}

void LRUKNode::Access(AccessType access_type, size_t timestamp) {
  history_.emplace_back(access_type, timestamp);
  if (history_.size() > k_) {
    history_.erase(history_.begin());
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (evictable_.empty()) {
    return false;
  }
  auto node_iter = evictable_.begin();
  *frame_id = (*node_iter)->GetFId();
  evictable_.erase(node_iter);
  node_store_.erase(*frame_id);
  curr_size_--;
  replacer_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    auto node = std::make_unique<LRUKNode>(frame_id, k_);
    node_store_[frame_id] = std::move(node);
    curr_size_++;
  }
  auto &node = node_store_[frame_id];
  if(node->GetEvicted())
    evictable_.erase(node.get());
  node->Access(access_type, ++current_timestamp_);
  if(node->GetEvicted())
    evictable_.insert(node.get());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (set_evictable && !node->GetEvicted()) {
    replacer_size_++;
    evictable_.insert(node.get());
  }
  if (!set_evictable && node->GetEvicted()) {
    replacer_size_--;
    evictable_.erase(node.get());
  }
  node->SetEvicted(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  evictable_.erase(node.get());
  node_store_.erase(frame_id);
  curr_size_--;
  replacer_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  return replacer_size_;
}

}  // namespace bustub
