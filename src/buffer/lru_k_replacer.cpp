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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (k_frames_.empty() && less_k_frames_.empty()) {
    return false;
  }

  // Helper function to get an evictable frame from a list
  auto get_evictable_from_list = [this](std::list<frame_id_t> &frames) -> frame_id_t {
    for (auto it = frames.begin(); it != frames.end(); ++it) {
      if (node_store_[*it].is_evictable_) {
        frame_id_t evictable_frame = *it;
        frames.erase(it);
        return evictable_frame;
      }
    }
    return -1;  // Indicates no evictable frame was found
  };

  frame_id_t to_evict = -1;
  if (!less_k_frames_.empty()) {
    to_evict = get_evictable_from_list(less_k_frames_);
  }

  if (to_evict == -1 && !k_frames_.empty()) {
    to_evict = get_evictable_from_list(k_frames_);
  }

  // If no evictable frame was found in both lists
  if (to_evict == -1) {
    return false;
  }

  *frame_id = to_evict;
  node_store_.erase(to_evict);
  curr_size_--;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame_id provided.");
  }

  current_timestamp_++;  // Increase the current timestamp.

  // If the frame is not already in node_store_, add it.
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node;
    node_store_[frame_id] = new_node;
    less_k_frames_.push_back(frame_id);  // since it's a new frame, put it in the less_k_frames_ list.
  }

  LRUKNode &accessed_node = node_store_[frame_id];
  accessed_node.history_.push_back(current_timestamp_);
  if (accessed_node.history_.size() > k_) {
    accessed_node.history_.pop_front();  // Maintain only k history entries.
  }

  // Move the frame to the end of the appropriate list to indicate recent access.
  if (accessed_node.history_.size() < k_) {
    less_k_frames_.remove(frame_id);     // remove the frame from its current position in the list
    less_k_frames_.push_back(frame_id);  // add it to the end of the list
  } else if (accessed_node.history_.size() == k_) {
    // Check if the node is already in k_frames_ to avoid unnecessary remove and add
    if (std::find(k_frames_.begin(), k_frames_.end(), frame_id) == k_frames_.end()) {
      less_k_frames_.remove(frame_id);
      k_frames_.push_back(frame_id);
    } else {
      k_frames_.remove(frame_id);
      k_frames_.push_back(frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  // Throw exception if frame_id is invalid.
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame_id provided.");
  }

  // Check if the frame exists in the node store.
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].is_evictable_ = set_evictable;
  } else {
    // If frame doesn't exist, you can either ignore or throw another exception.
    throw std::runtime_error("Frame_id does not exist in the replacer.");
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Throw exception if frame_id is invalid.
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame_id provided.");
  }

  // Check and remove from node_store_.
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    node_store_.erase(it);
    curr_size_--;
  }

  // Also remove the frame from k_frames_ or less_k_frames_.
  k_frames_.remove(frame_id);
  less_k_frames_.remove(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);

  size_t evictable_count = 0;
  for (const auto &pair : node_store_) {
    if (pair.second.is_evictable_) {
      evictable_count++;
    }
  }

  return evictable_count;
}

}  // namespace bustub
