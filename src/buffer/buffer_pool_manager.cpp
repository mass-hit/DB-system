//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetFrame(frame_id_t* frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id= free_list_.back();
    free_list_.pop_back();
  }
  else if(replacer_->Evict(frame_id)) {
    WritePage(&pages_[*frame_id]);
    page_table_.erase(pages_[*frame_id].page_id_);
  }
  else {
    return false;
  }
  return true;
}

void BufferPoolManager::InitPage(frame_id_t frame_id,page_id_t page_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_=page_id;
  pages_[frame_id].pin_count_=1;
  pages_[frame_id].is_dirty_=false;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);
  page_table_.emplace(page_id,frame_id);
}

void BufferPoolManager::Schedule(bool is_write_, Page *page) {
  auto promise=disk_scheduler_->CreatePromise();
  auto future=promise.get_future();
  disk_scheduler_->Schedule({is_write_,page->data_,page->page_id_,std::move(promise)});
  future.get();
}

void BufferPoolManager::WritePage(Page *page) {
  if(page->is_dirty_) {
    Schedule(true,page);
    page->is_dirty_=false;
  }
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if(!GetFrame(&frame_id)){
    return nullptr;
  }
  *page_id=AllocatePage();
  InitPage(frame_id,*page_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    frame_id_t frame_id;
    if(!GetFrame(&frame_id)) {
      return nullptr;
    }
    InitPage(frame_id,page_id);
    Schedule(false,&pages_[frame_id]);
    return &pages_[frame_id];
  }
  frame_id_t frame_id=it->second;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id=it->second;
  if(pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if(--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id,true);
  }
  if(is_dirty) {
    pages_[frame_id].is_dirty_=is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    return false;
  }
  WritePage(&pages_[it->second]);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < pool_size_; ++i) {
    Page &page = pages_[i];
    if (page.is_dirty_) {
      futures.push_back(std::async(std::launch::async, [this, &page]() {
        WritePage(&page);
      }));
    }
  }
  for (auto &future : futures) {
    future.get();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it=page_table_.find(page_id);
  if(it == page_table_.end()) {
    return true;
  }
  auto frame_id=it->second;
  if(pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  WritePage(&pages_[frame_id]);
  page_table_.erase(it);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_=INVALID_PAGE_ID;
  pages_[frame_id].pin_count_=0;
  pages_[frame_id].is_dirty_=false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
