#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
void BasicPageGuard::Reset() {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.Reset();
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    Reset();
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.Reset();
  return *this;
}

BasicPageGuard::~BasicPageGuard(){Drop();};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  ReadPageGuard read_guard(bpm_, page_);
  Reset();
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  WritePageGuard write_guard(bpm_, page_);
  Reset();
  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept: guard_(std::move(that.guard_)){}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {Drop();}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept: guard_(std::move(that.guard_)){};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if(guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {Drop();}  // NOLINT

}  // namespace bustub
