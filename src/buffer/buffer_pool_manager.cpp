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
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(this->latch_);
  frame_id_t fid = -1;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if (free_list_.empty()) {
    if (!replacer_->Evict(&fid) || fid == -1) {
      return nullptr;
    }
    if (pages_[fid].is_dirty_ && pages_[fid].page_id_ != INVALID_PAGE_ID) {
      FlushPage(pages_[fid].GetPageId());
    }

    page_table_.erase(pages_[fid].page_id_);
  }
  *page_id = AllocatePage();
  pages_[fid].ResetMemory();
  pages_[fid].is_dirty_ = false;

  page_table_[*page_id] = fid;
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = *page_id;

  replacer_->Remove(fid);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = page_table_.find(page_id);
  frame_id_t fid;
  if (it == page_table_.end()) {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&fid)) {
        return nullptr;
      }
      if (pages_[fid].is_dirty_) {
        FlushPage(pages_[fid].GetPageId());
      }
      page_table_.erase(pages_[fid].page_id_);
    }

    pages_[fid].ResetMemory();
    pages_[fid].is_dirty_ = false;

    pages_[fid].page_id_ = page_id;
    page_table_[page_id] = fid;
    pages_[fid].pin_count_ = 1;

    DiskRequest drequest;
    drequest.is_write_ = false;
    drequest.data_ = pages_[fid].data_;
    drequest.page_id_ = page_id;

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    drequest.callback_ = std::move(promise);
    disk_scheduler_->Schedule(std::move(drequest));
    future.wait();

    replacer_->Remove(fid);
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    return &pages_[page_table_[page_id]];
  }
  replacer_->RecordAccess(page_table_[page_id]);
  replacer_->SetEvictable(page_table_[page_id], false);
  pages_[page_table_[page_id]].pin_count_++;
  return &pages_[page_table_[page_id]];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = page_table_.find(page_id);
  frame_id_t fid = -1;
  if (it == page_table_.end()) {
    return false;
  }
  fid = it->second;
  if (pages_[fid].pin_count_ <= 0) {
    return false;
  }
  pages_[fid].pin_count_--;

  if (pages_[fid].pin_count_ <= 0) {
    replacer_->SetEvictable(fid, true);
  }

  if (is_dirty) {
    pages_[fid].is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;

    DiskRequest drequest;
    drequest.is_write_ = true;
    drequest.data_ = pages_[fid].data_;
    drequest.page_id_ = pages_[fid].page_id_;

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    drequest.callback_ = std::move(promise);

    disk_scheduler_->Schedule(std::move(drequest));
    future.wait();
    pages_[fid].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    FlushPage(pages_[i].page_id_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t fid = it->second;
  if (pages_[fid].pin_count_ > 0) {
    return false;
  }
  if (pages_[fid].is_dirty_) {
    FlushPage(page_id);
  }
  pages_[fid].ResetMemory();
  pages_[fid].is_dirty_ = false;

  page_table_.erase(it);
  pages_[fid].pin_count_++;
  pages_[fid].page_id_ = INVALID_PAGE_ID;

  replacer_->Remove(fid);
  free_list_.push_back(fid);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *fetched_page = FetchPage(page_id);
  // fetched_page->RLatch();
  return {this, fetched_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *fetched_page = FetchPage(page_id);
  // fetched_page->WLatch();
  return {this, fetched_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub