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
#include <climits>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { curr_size_ = 0; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  const auto t = GetCurrentTimeStamp();
  if (curr_size_ == 0) {
    return false;
  }
  bool is_first = true;
  size_t val = 0;
  size_t tmp;
  LRUKNode curr_node(0, 0);
  // empty LRUKNode is not evictable, always "<" than any evictable node
  for (const auto &[frame_id_node, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }
    if (is_first) {
      val = node.GetKDistance(t);
      *frame_id = node.fid_;
      is_first = false;
      curr_node = node;
    } else {
      tmp = node.GetKDistance(t);
      if ((tmp > val) || (tmp == val && val == ULONG_MAX && node.history_.front() < curr_node.history_.front())) {
        *frame_id = node.fid_;
        val = tmp;
        curr_node = node;
      }
    }
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT((0 <= frame_id) && (frame_id <= static_cast<int>(replacer_size_)), "Invalid frame id");
  std::lock_guard<std::mutex> lock_guard(latch_);
  const auto t = GetCurrentTimeStamp();
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    LRUKNode node(frame_id, k_);
    node.RecordAccess(t);
    node_store_.emplace(frame_id, std::move(node));
  } else {
    node_it->second.RecordAccess(t);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it == node_store_.end()) {
    return;
  }
  if (node_it->second.is_evictable_ != set_evictable) {
    node_it->second.is_evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto node_it = node_store_.find(frame_id);
  if (node_it != node_store_.end()) {
    curr_size_ += node_it->second.is_evictable_ ? -1 : 0;
    node_store_.erase(node_it);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
