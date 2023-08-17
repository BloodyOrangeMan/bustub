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
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

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
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  frame_id_t victim_frame;

  if (!free_list_.empty()) {
    victim_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&victim_frame);
  }

  if (pages_[victim_frame].IsDirty()) {
    disk_manager_->WritePage(pages_[victim_frame].page_id_, pages_[victim_frame].data_);
    page_table_.erase(pages_[victim_frame].page_id_);
    pages_[victim_frame].ResetMemory();
    DeallocatePage(pages_[victim_frame].GetPageId());
  }

  pages_[victim_frame].page_id_ = *page_id = AllocatePage();
  pages_[victim_frame].pin_count_ = 1;

  replacer_->RecordAccess(victim_frame);
  replacer_->SetEvictable(victim_frame, false);

  page_table_[pages_[victim_frame].page_id_] = victim_frame;

  return &pages_[victim_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  if (page_table_.find(page_id) != page_table_.end()) {
    pages_[page_table_[page_id]].pin_count_++;
    return &pages_[page_table_[page_id]];
  }

  frame_id_t victim_frame;

  if (!free_list_.empty()) {
    victim_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&victim_frame);
  }

  if (pages_[victim_frame].is_dirty_) {
    disk_manager_->WritePage(pages_[victim_frame].page_id_, pages_[victim_frame].data_);
    pages_[victim_frame].is_dirty_ = false;
  }

  disk_manager_->ReadPage(page_id, pages_[victim_frame].data_);

  pages_[victim_frame].pin_count_ = 1;
  pages_[victim_frame].page_id_ = page_id;

  replacer_->RecordAccess(victim_frame);
  replacer_->SetEvictable(victim_frame, false);

  page_table_[pages_[victim_frame].page_id_] = victim_frame;

  return &pages_[victim_frame];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t unpinned;

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  unpinned = page_table_[page_id];

  if (pages_[unpinned].pin_count_ <= 0) {
    return false;
  }

  pages_[unpinned].pin_count_--;

  if (pages_[unpinned].pin_count_ == 0) {
    replacer_->SetEvictable(unpinned, true);
  }

  if (is_dirty) {
    pages_[unpinned].is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);

  pages_[page_table_[page_id]].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPage(frame_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
