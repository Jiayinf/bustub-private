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
#include <algorithm>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (frame_id == nullptr) {
    throw bustub::Exception("frame_id null pointer invalid");
  }

  if (curr_size_ == 0) {
    return false;
  }
  size_t max_dist = 0;
  frame_id_t candidate = -1;
  bool found_infinite = false;
  for (auto &pair : node_store_) {
    auto &node = pair.second;
    if (!node.is_evictable_ || node.history_.size() < k_) {
      if (node.is_evictable_ && node.history_.size() < k_) {
        if (!found_infinite) {
          max_dist = std::numeric_limits<size_t>::max();
          candidate = node.fid_;
          found_infinite = true;
        } else {
          if (node.history_.front() < node_store_[candidate].history_.front()) {
            candidate = node.fid_;
          }
        }
      }
      continue;
    }

    size_t dist = current_timestamp_ - node.history_.front();

    if (dist > max_dist) {
      max_dist = dist;
      candidate = node.fid_;
    }
  }

  if (candidate != -1) {
    *frame_id = candidate;
    node_store_.erase(candidate);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);

  if (replacer_size_ <= static_cast<size_t>(frame_id) || frame_id < 0) {
    throw bustub::Exception("frame_id invalid");
  }
  current_timestamp_++;
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    LRUKNode cur;
    cur = LRUKNode(k_, frame_id);
    cur.history_.push_back(current_timestamp_);
    node_store_.emplace(frame_id, std::move(cur));
  } else {
    auto &cur = it->second;
    cur.AddHistory(current_timestamp_);
  }
}

// void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
//   std::unique_lock<std::mutex> lock(latch_);
//   if (replacer_size_ < static_cast<size_t>(frame_id) || frame_id < 0) {
//     throw bustub::Exception("frame_id invalid");
//   }
//   auto it = node_store_.find(frame_id);
//   LRUKNode cur;
//   if (it != node_store_.end()) {
//     cur = node_store_[frame_id];
//   } else {

//     cur = LRUKNode(k_, frame_id);
//   }

//   cur.AddHistory(current_timestamp_);

//   size_t curk = cur.GetSize();

//   if (curk < k_) {
//     cur.SetInf(true);
//     auto i = std::find(history_list_.begin(), history_list_.end(), frame_id);

//     if (i == history_list_.end()) {
//       history_list_.push_back(frame_id);
//     } else {
//       history_list_.splice(history_list_.end(), history_list_, i);
//     }
//   } else if (cur.IsInf()) {
//     history_list_.remove(frame_id);
//     cache_list_.push_back(frame_id);
//     cur.SetInf(false);
//   } else {
//     auto j = std::find(cache_list_.begin(), cache_list_.end(), frame_id);
//     cache_list_.splice(cache_list_.end(), cache_list_, j);
//   }

//   ++current_timestamp_;
//   node_store_[frame_id] = cur;
// }

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    throw bustub::Exception("frame_id invalid");
  }
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    throw bustub::Exception("frame_id invalid");
  }

  if (it->second.is_evictable_ == set_evictable) {
    return;
  }

  if (set_evictable && (!it->second.is_evictable_)) {
    ++curr_size_;
  } else if (!set_evictable && it->second.is_evictable_) {
    --curr_size_;
  }
  it->second.is_evictable_ = set_evictable;
  // node_store_[frame_id] = cur;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  if (replacer_size_ <= static_cast<size_t>(frame_id)) {
    throw bustub::Exception("frame_id invalid");
  }
  auto cur = node_store_[frame_id];
  // if (!cur.IsEvictable()) {
  //   throw bustub::Exception("is_evictable_ invalid");
  // }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  // cur.SetEvictable(false);
  if (it->second.is_evictable_) {
    curr_size_--;
  }
  node_store_.erase(frame_id);
  // if (cur.IsInf()) {
  //   history_list_.remove(frame_id);
  // } else {
  //   cache_list_.remove(frame_id);
  // }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

LRUKNode::LRUKNode(size_t k, frame_id_t frame_id) : k_(k), fid_(frame_id) {}
LRUKNode::LRUKNode() = default;

void LRUKNode::AddHistory(size_t history) {
  history_.push_back(history);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}

void LRUKNode::RemoveHistory() { history_.pop_front(); }

// auto LRUKNode::GetSize() -> size_t { return this->history_.size(); }

// auto LRUKNode::IsEvictable() -> bool { return this->is_evictable_; }

// auto LRUKNode::SetEvictable(bool ev) -> void { this->is_evictable_ = ev; }

// auto LRUKNode::SetInf(bool k) -> void { this->is_inf_ = k; }

// auto LRUKNode::IsInf() -> bool { return this->is_inf_; }

}  // namespace bustub