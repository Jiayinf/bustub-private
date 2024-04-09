#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this == &that) {
    that.bpm_ = nullptr;
  } else {
    this->bpm_ = that.bpm_;
  }
  this->page_ = that.page_;
  that.page_ = nullptr;
  //   this->bpm_ = that.bpm_;
  //   that.bpm_ = nullptr;
  is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    this->page_ = that.page_;
    that.page_ = nullptr;
    this->bpm_ = that.bpm_;
    that.bpm_ = nullptr;
    is_dirty_ = that.is_dirty_;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ != nullptr) {
    ReadPageGuard read_guard(bpm_, page_);
    // this->page_->RLatch();
    this->page_ = nullptr;
    this->bpm_ = nullptr;
    this->is_dirty_ = false;

    return read_guard;
  }
  throw std::runtime_error("error");
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ != nullptr) {
    WritePageGuard write_guard(bpm_, page_);
    // this->page_->WLatch();
    this->page_ = nullptr;
    this->bpm_ = nullptr;
    this->is_dirty_ = false;
    return write_guard;
  }
  throw std::runtime_error("error");
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  guard_ = BasicPageGuard(bpm, page);

  if (guard_.page_ != nullptr) {
    guard_.page_->RLatch();
  }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  guard_ = BasicPageGuard(bpm, page);

  if (guard_.page_ != nullptr) {
    guard_.page_->WLatch();
  }
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub